from transaction import TransactionType

class Variable:
  def __init__(self, var_id, value):
    self._id                = var_id
    self._committed_values  = [(0, value)]
    self._uncommitted_value = None


  def write(self, transaction, value):
    self._uncommitted_value = (transaction.get_id(), value)


  def commit(self, timestamp):
    self._committed_values.append((timestamp, self._uncommitted_value))


  def read_uncommitted(self, transaction):
    if self._uncommitted_value[0] == transaction.get_id():
      return self._uncommitted_value[1]

    # Transaction has write lock for this var, but hasn't written to it yet.
    return self.read_committed(transaction)


  def read_committed(self, transaction=None):
    if transaction and transaction.get_type == TransactionType.READ_ONLY:
      print 'READ_ONLY committed'
      for tick, val in self._committed_values:
        if tick <= transaction.get_ts():
          retval = val
        else:
          break
      return retval
    else:
      print 'READ_WRITE committed'
      return self._committed_values[-1][1]


  # Used for recovery
  def dump_committed(self):
    return self._committed_values


  def dump_uncommitted(self):
    return self._uncommitted_value


  def load_committed(self, values):
    self._committed_values = values


  def load_uncommitted(self, value):
    self._uncommitted_value = value


  def load_state(self, committed_values, uncommitted_values):
    self.load_committed(committed_values)
    self.load_uncommitted(uncommitted_values)
