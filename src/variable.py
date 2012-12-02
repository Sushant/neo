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
    if not transaction or transaction.get_type == TransactionType.READ_WRITE:
      return self._committed_values[-1][1]
    else:
      for tick, val in self._committed_values:
        if tick <= transaction.get_ts():
          retval = val
        else:
          break
      return retval
