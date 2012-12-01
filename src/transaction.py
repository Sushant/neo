class TransactionType:
  READ_ONLY  = 0
  READ_WRITE = 1

class TransactionStatus:
  ACTIVE  = 0
  ABORTED = 1
  WAITING = 2


class Transaction:
  def __init__(self, txn_id, ts, txn_type=TransactionType.READ_WRITE,
      txn_status=TransactionStatus.ACTIVE):
    self._id     = txn_id
    self._ts     = ts   # Timestamp of transaction
    self._status = txn_status
    self._type   = txn_type


  def get_id(self):
    return self._id


  def get_ts(self):
    return self._ts


  def get_status(self):
    return self._status


  def get_type(self):
    return self._type


  def wait(self):
    if self._status != TransactionStatus.WAITING:
      self._status = TransactionStatus.WAITING
