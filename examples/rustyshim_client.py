import pyarrow.flight as pf

class RustyShimConnection:
    class AuthHandler(pf.ClientAuthHandler):
        def __init__(self, username, password, request_admin):
            self.username = username
            self.password = password
            self.request_admin = request_admin
            self.token = None
        def authenticate(self, outgoing, incoming):
            outgoing.write(self.username)
            outgoing.write(self.password)
            outgoing.write(str(int(self.request_admin)))
            self.token = incoming.read()
        def get_token(self):
            if self.token is None:
                raise pf.FlightUnauthenticatedError("Not authenticated via SciDB")
            return self.token
    
    def __init__(self, host, username, password, request_admin, port, scheme):
        location = scheme + "://" + host + ":" + str(port)
        self.client = pf.connect(location)
        h = RustyShimConnection.AuthHandler(username, password, request_admin)
        self.client.authenticate(h)
        self.options = pf.FlightCallOptions(headers=[(b'authorization',h.get_token())])
    
    def list_actions(self):
        return self.client.list_actions(self.options)
    
    def refresh_context(self):
        return [r.body.to_pybytes().decode("utf-8") for r in self.client.do_action("REFRESH_CONTEXT", self.options)]

    def clear_expired_items(self):
        return [r.body.to_pybytes().decode("utf-8") for r in self.client.do_action("CLEAR_EXPIRED_ITEMS", self.options)]
    
    def get_sql(self, query):
        fd = pf.FlightDescriptor.for_path(query)
        fi = self.client.get_flight_info(fd, self.options)
        ep = fi.endpoints[0]
        return self.client.do_get(ep.ticket, self.options)

def rustyshim_connect(host, username, password, request_admin=False, port=50051, scheme = "grpc+tcp"):
    return RustyShimConnection(host, username, password, request_admin, port, scheme)