import threading
import arrow
import queue

from http.server import BaseHTTPRequestHandler, HTTPServer

from collections import namedtuple

HttpEvent = namedtuple("HttpEvent", "event_type timestamp command headers url client_ip body")


def HttpEventCollectorFact(q):
    class HttpEventCollector(BaseHTTPRequestHandler):
        def do_both(self):
            http_event = HttpEvent(
                'HTTP',
                arrow.utcnow().timestamp,
                self.command,
                repr(dict(self.headers)),
                self.path,
                repr(self.client_address),
                "" if self.command == "GET" else
                self.rfile.read(int(self.headers['Content-Length']))
            )
            self.q.put(http_event)
            self.send_response(200)

            self.send_header('Content-type', 'text/plain')
            self.end_headers()

            self.wfile.write(bytes(str(http_event.timestamp) + "\n", "utf8"))

    HttpEventCollector.q = q
    HttpEventCollector.do_POST = HttpEventCollector.do_both
    HttpEventCollector.do_GET = HttpEventCollector.do_both

    return HttpEventCollector


def start_http_thread(q, port):
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, HttpEventCollectorFact(q))
    httpd_thread = threading.Thread(target=httpd.serve_forever)
    httpd_thread.daemon = True
    httpd_thread.start()


def mk_http_queue(port):
    q = queue.Queue()
    start_http_thread(q, port)
    return q


def main():
    q = mk_http_queue(8081)

    while 1:
        event = q.get(block=True)
        print(event)


if __name__ == '__main__':
    main()
