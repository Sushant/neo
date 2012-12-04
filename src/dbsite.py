from variable import Variable
from lock_manager import LockManager
from transaction import TransactionType


class SiteStatus:
  UP   = 0
  DOWN = 1


class Site:
  def __init__(self, site_id):
    self._id     = site_id
    self._status = SiteStatus.UP
    self._init_variables()
    self._lm     = LockManager(self._site_variables.keys())


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


  def dump_state(self):
    return self._site_variables


  def _load_state(self, state):
    for var_id, var_obj in state.iteritems():
      if var_id in self._site_variables:
        uncommitted_values = var_obj.dump_uncommitted()
        committed_values = var_obj.dump_committed()
        new_var_obj = Variable(var_id, committed_values[0])
        new_var_obj.load_state(committed_values, uncommitted_values)
        self._site_variables[var_id] = new_var_obj

  def is_up(self):
    return self._status == SiteStatus.UP


  def fail(self):
    if self.is_up():
      self._status = SiteStatus.DOWN
      self._lm.release_all_locks()


  def recover(self, state):
    if not self.is_up():
      #self._load_state(state)
      for var_id, var_obj in self._site_variables.iteritems():
        if var_obj.is_replicated():
          var_obj.recover()
      self._status = SiteStatus.UP


  def write(self, transaction, variable, value):
    if variable in self._site_variables:
      self._lm.acquire_write_lock(transaction, variable)
      self._site_variables[variable].write(transaction, value)
    else:
      print 'Variable doesn\'t exist on site'


  def read(self, transaction, var_id):
    if var_id in self._site_variables:
      if self._site_variables[var_id].is_recovering():
        return None
      if self._lm.txn_has_write_lock(transaction, var_id):
        return self._site_variables[var_id].read_uncommitted(transaction)
      else:
        if transaction.get_type() != TransactionType.READ_ONLY:
          self._lm.acquire_read_lock(transaction, var_id)
        return self._site_variables[var_id].read_committed(transaction)
    else:
      return None


  def commit(self, transaction, timestamp):
    for var in self._site_variables.keys():
      if self._lm.txn_has_write_lock(transaction, var):
        self._site_variables[var].commit(timestamp)
    self._lm.release_all_locks(transaction)


  def abort(self, transaction):
    self._lm.release_all_locks(transaction)
