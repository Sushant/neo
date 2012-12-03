from dbsite import Site
from transaction import Transaction
from transaction import TransactionType
from transaction import TransactionStatus
from lock_manager import AcquireLockException

from pprint import pprint

MAX_SITES = 10
MAX_VARS  = 20

class TransactionManager:
  def __init__(self):
    self._sites = {}
    self._init_sites()
    self._ts = 0
    self._txn_sites = {}
    self._transactions = {}
    self._wait_list = []


  def inc_ts(self):
    self._ts += 1


  def _init_sites(self):
    for i in xrange(MAX_SITES):
      self._sites[i] = Site(i)


  def begin(self, txn_id):
    if txn_id not in self._transactions:
      self._transactions[txn_id] = Transaction(txn_id, self._ts)
      print 'Began transaction %s with time_stamp %d' % \
          (txn_id, self._transactions[txn_id].get_ts())
    else:
      raise Exception('Transaction with ID %s already exists' % txn_id)

  def beginRO(self, txn_id):
    if txn_id not in self._transactions:
      self._transactions[txn_id] = Transaction(txn_id, self._ts,
          txn_type=TransactionType.READ_ONLY)
      print 'Began transaction %s with time_stamp %d' % \
          (txn_id, self._transactions[txn_id].get_ts())
    else:
      raise Exception('Transaction with ID %s already exists' % txn_id)


  def _get_sites_for_var(self, var_id):
    if var_id > MAX_VARS:
      raise Exception('Unknown variable')
    elif (var_id % 2) == 0:
      return self._sites.keys()
    else:
      return [(var_id + 1) % 10]


  def write(self, txn_id, var, value):
    print 'Writing...'
    transaction = self._transactions[txn_id]
    if not transaction.is_aborted():
      var_id = int(var[1:])
      site_ids = self._get_sites_for_var(var_id)
      try:
        succeeded_writes = 0
        for s in site_ids:
          if not txn_id in self._txn_sites:
            self._txn_sites[txn_id] = [s]
          else:
            if not s in self._txn_sites[txn_id]:
              self._txn_sites[txn_id].append(s)
          if self._sites[s].is_up():
            self._sites[s].write(transaction, var, value)
            succeeded_writes += 1
        if succeeded_writes > 0:
          print 'Wrote var %s for txn %s at time_stamp %d' % \
              (var, txn_id, self._ts)
          if transaction.get_status() == TransactionStatus.WAITING:
            transaction.activate()
        else:
          # If we reach here, we weren't able to find a site to read from
          print 'Unable to write %s, no site available' % var
          self._wait_list.append(('write', txn_id, var, value))
          transaction.wait()
      except AcquireLockException as ale:
        conflicting_txn_id = ale.args[1]
        conflicting_txn = self._transactions[conflicting_txn_id]
        if transaction.get_ts() > conflicting_txn.get_ts():
          self._abort(transaction)
        else:
          if not transaction.get_status() == TransactionStatus.WAITING:
            transaction.wait()
            self._wait_list.append(('write', txn_id, var, value))
    else:
      print 'Transaction %s is in aborted state' % txn_id


  def read(self, txn_id, var):
    transaction = self._transactions[txn_id]
    if not transaction.is_aborted():
      var_id = int(var[1:])
      site_ids = self._get_sites_for_var(var_id)
      try:
        for s in site_ids:
          if self._sites[s].is_up():
            if transaction.get_type() == TransactionType.READ_WRITE:
              if not txn_id in self._txn_sites:
                self._txn_sites[txn_id] = [s]
              else:
                if not s in self._txn_sites[txn_id]:
                  self._txn_sites[txn_id].append(s)
            retval = self._sites[s].read(transaction, var)
            if retval:
              print 'Read var %s for txn %s at time_stamp %d, value: %s' \
                  % (var, txn_id, self._ts, repr(retval))
              if transaction.is_waiting():
                transaction.activate()
              return retval
        # If we reach here, we weren't able to find a site to read from
        print 'Unable to read %s, no site available' % var
        if not transaction.is_waiting():
          self._wait_list.append(('read', txn_id, var))
          transaction.wait()
      except AcquireLockException as ale:
        print 'Step 1'
        conflicting_txn_id = ale.args[1]
        conflicting_txn = self._transactions[conflicting_txn_id]
        if transaction.get_ts() > conflicting_txn.get_ts():
          print 'Step 2'
          self._abort(transaction)
        else:
          print 'Step 3'
          if not transaction.get_status() == TransactionStatus.WAITING:
            print 'Step 4'
            transaction.wait()
            self._wait_list.append(('read', txn_id, var))
    else:
      print 'Transaction %s is in aborted state' % txn_id


  def _abort(self, transaction):
    txn_id = transaction.get_id()
    if txn_id in self._txn_sites:
      for s in self._txn_sites[transaction.get_id()]:
        self._sites[s].abort(transaction)
        transaction.abort()
      print 'Transaction %s aborted at time_stamp %d' % (txn_id, self._ts)
      self._retry_waiting_txns()
    else:
      print 'Txn %s not found on transaction manager' % txn_id


  def _abort_site_txns(self, site_id):
    for txn_id, txn_sites in self._txn_sites.iteritems():
      if site_id in txn_sites:
        transaction = self._transactions[txn_id]
        self._abort(transaction)
        continue


  def _retry_waiting_txns(self):
    print 'Wait list: ', self._wait_list
    i = 0
    while i < len(self._wait_list):
      operation = self._wait_list[i]
      txn_id = operation[1]
      transaction = self._transactions[txn_id]
      if operation[0] == 'write':
        var, value = operation[2:]
        print 'Retrying write with args', repr(operation[1:])
        self.write(txn_id, var, value)
      elif operation[0] == 'read':
        var = operation[2]
        print 'Retrying read with args', repr(operation[1:])
        self.read(txn_id, var)
      if not transaction.is_waiting():
        self._wait_list.remove(operation)
      else:
        i += 1


  def end(self, txn_id):
    transaction = self._transactions[txn_id]
    if not transaction.is_aborted():
      if transaction.get_type() == TransactionType.READ_WRITE:
        for s in self._txn_sites[txn_id]:
          if self._sites[s].is_up():
            self._sites[s].commit(transaction, self._ts)
          else:
            print 'One of the site accessed by txn failed, aborting'
            self._abort(transaction)
            return
        self._retry_waiting_txns()
      print 'Ended txn %s at time_stamp %d' % (txn_id, self._ts)
    else:
      print 'Transaction %s is in aborted state' % txn_id


  def dump(self, site_id=None, var_id=None):
    site_var = {}
    if not var_id:
      if not site_id:
        for s, obj in self._sites.iteritems():
          site_var[s] = obj.dump()
        return site_var
      else:
        return self._sites[site_id].dump()
    else:
      if site_id:
        var_id = int(var_id[1:])
        return self._sites[site_id].dump(var_id)
      else:
        var_id = int(var_id[1:])
        sites = self._get_sites_for_var(var_id)
        for s in sites:
          return self._sites[s].dump(var_id)


  def fail(self, site_id):
    if site_id in self._sites:
      self._sites[site_id].fail()
      self._abort_site_txns(site_id)
    else:
      print 'Unknown site %s' % site_id

  def recover(self, site_id):
    for s in self._sites.values():
      if s.is_up():
        state = s.dump_state()
        if state:
          self._sites[site_id].recover(state)
          pprint('Other state: %s'  % repr(s.dump()))
          pprint('Recovered state: %s'  % repr(self.dump(site_id)))
          break
