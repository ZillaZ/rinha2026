module main

import fasthttp { HttpRequest, HttpResponse, ServerConfig }
import net
import x.json2
import v.util
import time
import math
import os
import zillaz.hnsw
import encoding.binary
import arrays

struct TransactionInfo {
	amount       f64
	installments i32
	requested_at string
}

struct Customer {
	avg_amount      f64
	tx_count_24h    i32
	known_merchants []string
}

struct Merchant {
	id         string
	mcc        string
	avg_amount f64
}

struct Terminal {
	is_online    bool
	card_present bool
	km_from_home f64
}

struct LastTransaction {
	timestamp       string
	km_from_current f64
}

struct Transaction {
	id               string
	transaction      TransactionInfo
	customer         Customer
	merchant         Merchant
	terminal         Terminal
	last_transaction ?LastTransaction
}

fn (self Transaction) vectorize() ![]f64 {
	parsed := time.parse_iso8601(self.transaction.requested_at)!
	mut minutes_since_last_tx := -1.0
	mut km_from_last_tx := -1.0

	if last_transaction := self.last_transaction {
		last := time.parse_iso8601(last_transaction.timestamp)!
		res := parsed - last
		minutes_since_last_tx = res.minutes() / normalization.max_minutes
		km_from_last_tx = last_transaction.km_from_current
	}

	is_online := if self.terminal.is_online { 1 } else { 0 }
	card_present := if self.terminal.card_present { 1 } else { 0 }
	unknown_merchant := if self.customer.known_merchants.filter(fn [self] (v string) bool {
		return self.merchant.id == v
	}).len != 0 {
		1
	} else {
		0
	}

	return [
		math.clamp(self.transaction.amount / normalization.max_amount, 0.0, 1.0),
		math.clamp(self.transaction.installments / normalization.max_installments, 0.0, 1.0),
		math.clamp((self.transaction.amount / self.customer.avg_amount) / normalization.amount_vs_average_ratio,
			0.0, 1.0),
		math.clamp(f64(parsed.hour) / 23, 0.0, 1.0),
		math.clamp(f64(parsed.day_of_week()) / 6, 0.0, 1.0),
		math.clamp(f64(minutes_since_last_tx), 0.0, 1.0),
		math.clamp(f64(km_from_last_tx) / normalization.max_km, 0.0, 1.0),
		math.clamp(self.terminal.km_from_home / normalization.max_km, 0.0, 1.0),
		math.clamp(self.customer.tx_count_24h / normalization.max_tx_count_24h, 0.0, 1.0),
		math.clamp(f64(is_online), 0.0, 1.0),
		math.clamp(f64(card_present), 0.0, 1.0),
		math.clamp(f64(unknown_merchant), 0.0, 1.0),
		mcc_risk[self.merchant.mcc] or { 0.5 } as f64,
		math.clamp(self.merchant.avg_amount / normalization.max_merchant_avg_amount, 0.0, 1.0),
	]
}

struct TransactionResult {
	approved    bool
	fraud_score f64
}

struct Normalization {
	max_amount              f64
	max_installments        f64
	amount_vs_average_ratio f64
	max_minutes             f64
	max_km                  f64
	max_tx_count_24h        f64
	max_merchant_avg_amount f64
}

const collection = 'rinha'
const collection_distance = 'Cosine'

fn get_reference(file os.File) !(Reference, i64) {
	mut line := []u8{len: 10000, cap: 30000, init: 0}
	bytes := file.read_bytes_with_newline(mut &line)!
	ret := json2.decode[Reference](line[..bytes].bytestr())!
	return ret, bytes
}

struct ReferenceIndex {
	vector []f64
	offset i64
}

const factor = 1000.0

struct Offset {
	start i64
mut:
	end i64
}

const workers = 4

fn gen_offsets(path string) []Offset {
	stat := os.stat(path) or { panic(err) }
	file := os.open(path) or { panic(err) }
	apprx_size := stat.size / workers
	mut line := []u8{len: 200, cap: 200, init: 0}
	mut end := i64(0)
	mut ret := []Offset{}
	for i in 0 .. workers {
		offset := apprx_size * i
		file.seek(offset, os.SeekMode.start) or { panic(err) }
		bytes := file.read_bytes_with_newline(mut &line) or { break }
		end = offset + i64(bytes)
		if i == 0 {
			ret << Offset{
				start: 0
				end:   0
			}
			continue
		}
		ret[i - 1].end = end
		ret << Offset{
			start: end
			end:   0
		}
	}
	ret[workers - 1].end = end
	return ret
}

fn dot_prod(one []f64, other []f64) f64 {
	mut dot := 0.0
	mut norm_a := 0.0
	mut norm_b := 0.0

	for i in 0 .. one.len {
		dot += one[i] * other[i]
		norm_a += one[i] * one[i]
		norm_b += other[i] * other[i]
	}

	if norm_a == 0 || norm_b == 0 {
		return 0.0
	}

	return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))
}

fn db_worker(channel chan []ReferenceIndex, offset Offset) {
	file := os.open('formatted.json') or { panic(err) }
	mut count := 0.0
	mut final := []ReferenceIndex{}
	file.seek(offset.start, os.SeekMode.start) or { panic(err) }
	mut acc_bytes := offset.start
	max := 100.0
	for {
		if count == max {
			break
		}
		reference, bytecount := get_reference(file) or { break }
		acc_bytes += bytecount
		final << ReferenceIndex{
			vector: reference.vector
			offset: acc_bytes
		}
		count += 1
	}
	channel <- final
}

fn init_db(mut db hnsw.HNSW[Reference]) {
	offsets := gen_offsets('formatted.json')
	channel := chan []ReferenceIndex{cap: workers}
	mut threads := []thread{}
	for i in 0 .. workers {
		copy := offsets[i]
		threads << spawn db_worker(channel, copy)
	}
	threads.wait()
	channel.close()
	mut final := []ReferenceIndex{}
	file := os.open('formatted.json') or { panic(err) }

	for {
		value := <-channel or { break }
		mut count := 0.0
		for index in value {
			count += 1.0
			file.seek(index.offset, os.SeekMode.start) or { panic(err) }
			reference, _ := get_reference(file) or { panic(err) }
			stopwatch := time.new_stopwatch(time.StopWatchOptions{})
			db.insert(reference)
			final << index
		}
	}
}

const default_ok = 'HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n'
const default_not_found = 'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n'
const default_bad_request = 'HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n'

const mcc_risk = json2.map_from(util.read_file('mcc_risk.json')!)
const normalization = json2.decode[Normalization](util.read_file('normalization.json')!)!

struct Reference {
	vector []f64
	label  string
}

pub fn (self Reference) to_bytes() []u8 {
	mut ret := [][]u8{}
	for x in self.vector {
		bits := math.f64_bits(x)
		aux := binary.big_endian_get_u64(bits)
		ret << aux
	}
	mut aux := self.label.bytes()
	for _ in 0 .. 6 - self.label.len {
		aux << 0
	}
	ret << aux

	fret := arrays.flatten(ret)
	return fret
}

pub fn Reference.from_bytes(bytes []u8) Reference {
	mut vector := []f64{}
	for i in 0 .. 14 {
		bits := binary.big_endian_u64_at(bytes, 8 * i)

		vector << math.f64_from_bits(bits)
	}
	label := bytes[14 * 8..14 * 8 + 6].bytestr()
	return Reference{
		vector: vector
		label:  label
	}
}

pub fn Reference.byte_size() u64 {
	return 14 * 8 + 6
}

pub fn (self Reference) distance_to(other Reference) f64 {
	mut distance := 0.0
	for i in 0 .. 14 {
		distance += math.pow(self.vector[i] - other.vector[i], 2.0)
	}
	return math.sqrt(distance)
}

struct MyResponse {
	status         int
	status_message string
	headers        map[string]string
	body           string
}

fn (self MyResponse) serialize() string {
	mut serialized := ['HTTP/1.1 ${self.status} ${self.status_message}']
	for key, value in self.headers {
		serialized << '${key}: ${value}'
	}
	serialized << '\r\n'
	serialized << self.body
	ret := serialized.join('\r\n')
	return ret
}

struct ApiResponse {
	approved    bool
	fraud_score f64
}

@[unsafe]
fn (t &Transaction) free() {
	unsafe {
		t.id.free()
		t.transaction.free()
		t.customer.free()
		t.merchant.free()
		t.terminal.free()
		if last := t.last_transaction {
			last.free()
		}
	}
}

@[unsafe]
fn (t &TransactionInfo) free() {
	unsafe { t.requested_at.free() }
}

@[unsafe]
fn (c &Customer) free() {
	unsafe {
		c.known_merchants.free()
	}
}

@[unsafe]
fn (m &Merchant) free() {
	unsafe {
		m.id.free()
		m.mcc.free()
	}
}

@[unsafe]
fn (t &Terminal) free() {}

@[unsafe]
fn (l &LastTransaction) free() {
	unsafe { l.timestamp.free() }
}

@[unsafe]
fn (r &Reference) free() {
	unsafe {
		r.vector.free()
		r.label.free()
	}
}

@[manualfree]
fn server_handler(req HttpRequest) !HttpResponse {
	method := req.buffer[req.method.start..req.method.start + req.method.len].bytestr()
	path := req.buffer[req.path.start..req.path.start + req.path.len].bytestr()
	body := req.buffer[req.body.start..req.body.start + req.body.len].bytestr()
	mut ret := HttpResponse{
		content: default_bad_request.bytes()
	}
	if method == 'GET' && path == '/ready' {
		ret.content = default_ok.bytes()
	} else if method == 'POST' && path == '/fraud-score' {
		if transaction := json2.decode[Transaction](body) {
			mut db := &hnsw.HNSW[Reference](req.user_data)
			mut query := Reference{
				vector: transaction.vectorize()!
				label:  ''
			}
			points := db.knn_search(query, 5, 50)
			unsafe { query.vector.free() }
			fraud_count := points.filter(fn (p Reference) bool {
				return p.label.contains('fraud')
			}).len
			fraud_score := f64(fraud_count) / 5.0
			payload := json2.encode(ApiResponse{
				approved:    fraud_score < 0.6
				fraud_score: fraud_score
			})

			ret.content =
				'HTTP/1.1 200 OK\r\nContent-Length: ${payload.len}\r\n\r\n${payload}'.bytes()
			unsafe {
				points.free()
				transaction.free()
				payload.free()
			}
		}
	} else {
		ret.content = default_not_found.bytes()
	}
	return ret
}

fn main() {
	println('Initializing vector database...')
	mut db := hnsw.HNSW[Reference]{}
	if os.args.len > 1 && os.args[1] == 'init' {
		db = hnsw.new_hnsw[Reference](5, 400)
		init_db(mut &db)
		db.snapshot()!
		println('Database initialized successfully.')
		return
	}
	db = hnsw.HNSW.load_from_memory[Reference]()!
	server := fasthttp.new_server(ServerConfig{
		family:    net.AddrFamily.ip
		port:      9999
		handler:   server_handler
		user_data: &db
	})!
	server.run()!
}
