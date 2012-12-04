import sys
from variable import Variable
from lock_manager import LockManager
from transaction import TransactionType
from lock_manager import AcquireLockException

from SimpleXMLRPCServer import SimpleXMLRPCServer

class SiteStatus:
  UP   = 0
  DOWN = 1


class Site:
  def __init__(self, site_id, port=3590):
    self._id     = site_id
    self._status = SiteStatus.UP
    self._init_variables()
    self._lm     = LockManager(self._site_variables.keys())
    self._init_server(port)


  def _init_server(self, port):
    self._server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
    self._server.register_function(self.get_id)
    self._server.register_function(self.dump)
    self._server.register_function(self.is_up)
    self._server.register_function(self.fail)
    self._server.register_function(self.recover)
    self._server.register_function(self.write)
    self._server.register_function(self.read)
    self._server.register_function(self.commit)
    self._server.register_function(self.abort)
    self._server.serve_forever()


  def _init_variables(self):
    self._site_variables = {}
    for i in xrange(1, 21):
      var_id = 'x' + str(i)
      # value  = int('10' + str(i))
      value = 10 * i
      if i % 2 == 0:  # Even numbered variables are replicated
        self._site_variables[var_id] = Variable(var_id, value)
      elif (i + 1) % 10 == self._id:
        self._site_variables[var_id] = Variable(var_id, value)


  def get_id(self):
    return self._id


  def dump(self, var=None):
    if var:
      if var in self._site_variables:
        return {var: self._site_variables[var].read_committed()}
      print 'Variable doesn\'t exist on site'
    else:
      var_dict = {}
      for var, obj in self._site_variables.iteritems():
        var_dict[var] = obj.read_committed()
      return var_dict


  def is_up(self):
    return self._status == SiteStatus.UP


  def fail(self):
    if self.is_up():
      self._status = SiteStatus.DOWN
      self._lm.release_all_locks()


  def recover(self):
    if not self.is_up():
      #self._load_state(state)
      for var_id, var_obj in self._site_variables.iteritems():
        if var_obj.is_replicated():
          var_obj.recover()
      self._status = SiteStatus.UP


  def write(self, transaction, variable, value):
    try:
      if variable in self._site_variables:
        self._lm.acquire_write_lock(transaction, variable)
        self._site_variables[variable].write(transaction, value)
        return {'status': 'success'}
      else:
        return {'status': 'error', 'msg': 'Variable doesn\'t exist on site'}
    except AcquireLockException as ale:
      return {'status': 'exception', 'args': ale.args}


  def read(self, transaction, var_id):
    try:
      if var_id in self._site_variables:
        if self._site_variables[var_id].is_recovering():
          return {'status': 'success', 'data': None}
        if self._lm.txn_has_write_lock(transaction, var_id):
          return {'status': 'success',
              'data': self._site_variables[var_id].read_uncommitted(transaction)}
        else:
          if transaction['_type'] != TransactionType.READ_ONLY:
            self._lm.acquire_read_lock(transaction, var_id)
          return {'status': 'success',
              'data': self._site_variables[var_id].read_committed(transaction)}
      else:
        return {'status': 'error', 'data': None}
    except AcquireLockException as ale:
      return {'status': 'exception', 'args': ale.args}


  def commit(self, transaction, timestamp):
    for var in self._site_variables.keys():
      if self._lm.txn_has_write_lock(transaction, var):
        self._site_variables[var].commit(timestamp)
    self._lm.release_all_locks(transaction)


  def abort(self, transaction):
    self._lm.release_all_locks(transaction)


if __name__ == '__main__':
  if len(sys.argv) != 3:
      print 'USAGE: python %s <site-id> <port>' % (sys.argv[0])
      sys.exit(1)
  site_id= int(sys.argv[1])
  port = int(sys.argv[2])
  dbsite = Site(site_id, port)
