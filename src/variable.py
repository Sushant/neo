from transaction import TransactionType


# Used when a site recovers and it doesn't want to allow reads to 
# it's replicated data
class VariableStatus:
  ACTIVE     = 0
  RECOVERING = 1


class Variable:
  def __init__(self, var_id, value):
    self._id                = var_id
    self._committed_values  = [(0, value)]
    self._uncommitted_value = None
    self._status = VariableStatus.ACTIVE


  def write(self, transaction, value):
    self._uncommitted_value = (transaction['_id'], value)


  def commit(self, timestamp):
    if self.is_recovering():
      self._status = VariableStatus.ACTIVE
    self._committed_values.append((timestamp, self._uncommitted_value))


  def read_uncommitted(self, transaction):
    if self._uncommitted_value[0] == transaction['_id']:
      return self._uncommitted_value[1]

    # Transaction has write lock for this var, but hasn't written to it yet.
    return self.read_committed(transaction)


  def read_committed(self, transaction=None):
    if transaction and transaction['_type'] == TransactionType.READ_ONLY:
      for tick, val in self._committed_values:
        if tick <= transaction['_ts']:
          retval = val
        else:
          break
      return retval
    else:
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

  def recover(self):
    self._status = VariableStatus.RECOVERING

  def is_recovering(self):
    return self._status == VariableStatus.RECOVERING


  def is_replicated(self):
    var_index = int(self._id[1:])
    return (var_index % 2) == 0
