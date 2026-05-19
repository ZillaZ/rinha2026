module main

import fasthttp { HttpRequest, HttpResponse, ServerConfig }
import net
import x.json2
import v.util
import time
import math
import net.http

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

struct QDrantBatch {
	ids      []int
	vectors  [][]f64
	payloads []map[string]string
}

struct QDrantBatchReq {
	batch QDrantBatch
}

fn init() {
	res := http.put('http://localhost:6333/collections/rinha',
		'{"vectors": { "size": 14, "distance": "Cosine" }}') or { panic(err) }
	println(res)

	mut ids := []int{}
	for i, _ in references {
		ids << i
	}
	payload := QDrantBatchReq{
		batch: QDrantBatch{
			ids:      ids
			vectors:  references.map(fn (v Reference) []f64 {
				return v.vector
			})
			payloads: references.map(fn (v Reference) map[string]string {
				return {
					'label': v.label
				}
			})
		}
	}

	pres := http.put('http://localhost:6333/collections/rinha/points', json2.encode(payload)) or {
		panic(err)
	}
	println(pres)
}

struct QDrantQuery {
	query        []f64
	limit        int
	with_payload bool
}

struct QDrantPoint {
	id      int
	payload map[string]string
}

struct QDrantQueryResult {
	points []QDrantPoint
}

struct QDrantQueryResponse {
	result QDrantQueryResult
}

fn get_closest(transaction Transaction) ![]main.QDrantPoint {
	vector := transaction.vectorize()!
	payload := QDrantQuery{
		query:        vector
		limit:        5
		with_payload: true
	}
	res := http.post('http://localhost:6333/collections/rinha/points/query', json2.encode(payload))!
	println(res)
	response := json2.decode[QDrantQueryResponse](res.body)!
	return response.result.points
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

const references = json2.decode[[]Reference](util.read_file('testdataset.json')!)!

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
	return serialized.join('\r\n')
}

struct ApiResponse {
	approved    bool
	fraud_score f64
}

fn server_handler(req HttpRequest) !HttpResponse {
	method := req.buffer[req.method.start..req.method.start + req.method.len].bytestr()
	path := req.buffer[req.path.start..req.path.start + req.path.len].bytestr()
	body := req.buffer[req.body.start..req.body.start + req.body.len].bytestr()

	if method == 'GET' && path == '/ready' {
		return HttpResponse{
			content: default_ok.bytes()
		}
	} else if method == 'POST' && path == '/fraud-score' {
		transaction := json2.decode[Transaction](body) or {
			return HttpResponse{
				content: default_bad_request.bytes()
			}
		}
		points := get_closest(transaction)!
		frauds := points.filter(fn (v QDrantPoint) bool {
			return v.payload['label'] == 'fraud'
		}).len
		score := f64(frauds) / 5.0
		payload := json2.encode(ApiResponse{
			approved:    score < 0.5
			fraud_score: score
		})
		response := MyResponse{
			status:         200
			status_message: 'OK'
			headers:        {
				'Content-Length': (payload.len + 2).str()
			}
			body:           payload
		}
		return HttpResponse{
			content: response.serialize().bytes()
		}
	} else {
		return HttpResponse{
			content: default_not_found.bytes()
		}
	}
}

fn main() {
	init()
	server := fasthttp.new_server(ServerConfig{
		family:  net.AddrFamily.ip
		port:    9999
		handler: server_handler
	})!
	server.run()!
}
