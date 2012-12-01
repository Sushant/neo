from site import Site
from transaction import Transaction
from transaction import TransactionType


MAX_SITES = 10
MAX_VARS  = 20

class TaskManager:
  def __init__(self):
    self._init_sites()
    self._ts = 0
    self._txn_sites = {}
    self._transactions = {}


  def inc_ts(self):
    self._ts += 1


  def _init_sites(self):
    for i in xrange(1, MAX_SITES + 1):
      self._sites[i] = Site(i)


  def begin(self, txn_id):
    if txn_id not in self._transactions:
      self._transactions[txn_id] = Transaction(txn_id, self._ts)
    else:
      raise Exception('Transaction with ID %s already exists' % txn_id)

  def beginRO(self, txn_id):
    if txn_id not in self._transactions:
      self._transactions[txn_id] = Transaction(txn_id, self._ts,
          txn_type=TransactionType.READ_ONLY)
    else:
      raise Exception('Transaction with ID %s already exists' % txn_id)


  def _get_sites_for_var(self, var_id):
    if var_id > MAX_VARS:
      raise Exception('Unknown variable')
    elif (var_id % 2) == 0:
      return self._sites.keys()
    else:
      return (var_id + 1) % 10


  def write(self, txn_id, var, value):
    transaction = self._transactions[txn_id]
    var_id = int(var[1:])
    site_ids = self._get_sites_for_var(var_id)
    for s in site_ids:
      if not s in self._txn_sites
        self._txn_sites[txn_id].append(s)
      self._sites[s].write(transaction, var, value)


  def end(self, txn_id):
    transaction = self._transactions[txn_id]
    for s in self._txn_sites[txn_id]:
      self._sites[s].commit(transaction)


  def dump(self, site_id=None, var_id=None):
    site_var = {}
    if not var_id:
      if not site_id:
        for s, obj in self._sites.iteritems():
          site_var[s] = obj.dump()
      else:
        self._sites[site_id].dump()
    else:
      if site_id:
        self._sites[site_id].dump(var_id)
      else:
        sites = self._get_sites_for_var(var_id)
        for s in sites:
          self._sites[s].dump(var_id)
