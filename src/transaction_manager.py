from dbsite import Site
from transaction import Transaction
from transaction import TransactionType
from lock_manager import AcquireLockException

from pprint import pprint
import sys
import re
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

MAX_SITES = 10
MAX_VARS  = 20
PORT      = 7777
SITE_PORTS = [9090, 9091, 9092, 9093, 9094, 9095, 9096, 9097, 9098, 9099]


class TransactionManager:
  def __init__(self):
    self._sites = {}
    self._init_sites()
    self._ts = 0
    self._txn_sites = {}
    self._transactions = {}
    self._wait_list = []
    self._init_server(PORT)


  def _init_server(self, port=PORT):
    self._server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
    self._server.register_function(self.inc_ts)
    self._server.register_function(self.begin)
    self._server.register_function(self.beginRO)
    self._server.register_function(self.write)
    self._server.register_function(self.read)
    self._server.register_function(self.end)
    self._server.register_function(self.dump_all)
    self._server.register_function(self.dump_var)
    self._server.register_function(self.dump_site)
    self._server.register_function(self.dump_site_var)
    self._server.register_function(self.fail)
    self._server.register_function(self.recover)
    self._server.serve_forever()


  def inc_ts(self):
    self._ts += 1


  def _init_sites(self):
    i = 0
    for port in SITE_PORTS:
      site_client = xmlrpclib.ServerProxy('http://localhost:%d' % port,
          allow_none=True)
      self._sites[i] = site_client
      i += 1


  def begin(self, txn_id):
    if txn_id not in self._transactions:
      self._transactions[txn_id] = Transaction(txn_id, self._ts)
      self._txn_sites[txn_id] = []
      return 'Began transaction %s with time_stamp %d' % \
          (txn_id, self._transactions[txn_id].get_ts())
    else:
      raise Exception('Transaction with ID %s already exists' % txn_id)


  def beginRO(self, txn_id):
    if txn_id not in self._transactions:
      self._transactions[txn_id] = Transaction(txn_id, self._ts,
          txn_type=TransactionType.READ_ONLY)
      return 'Began read-only transaction %s with time_stamp %d' % \
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
    transaction = self._transactions[txn_id]
    if not transaction.is_aborted():
      var_id = int(var[1:])
      site_ids = self._get_sites_for_var(var_id)
      succeeded_writes = 0
      for s in site_ids:
        if self._sites[s].is_up():
          self._add_to_txn_sites(transaction, s)
          retval = self._sites[s].write(transaction, var, value)
          if retval['status'] == 'exception':
            args = retval['args']
            return self._handle_wait_die(('write', txn_id, var, value), args[0], args[1])
          elif retval['status'] == 'success':
            succeeded_writes += 1
      if succeeded_writes > 0:
        return 'Wrote var %s for txn %s at time_stamp %d' % \
            (var, txn_id, self._ts)
        if transaction.is_waiting():
          transaction.activate()
      else:
        # If we reach here, we weren't able to find a site to read from
        return 'Unable to write %s, no site available' % var
        self._add_to_waitlist(('write', txn_id, var, value))
    else:
      return 'Transaction %s is in aborted state' % txn_id


  def _add_to_txn_sites(self, transaction, site_id):
    if transaction.get_type() == TransactionType.READ_WRITE:
      txn_id = transaction.get_id()
      if not site_id in self._txn_sites[txn_id]:
        self._txn_sites[txn_id].append(site_id)


  def read(self, txn_id, var):
    transaction = self._transactions[txn_id]
    if not transaction.is_aborted():
      var_id = int(var[1:])
      site_ids = self._get_sites_for_var(var_id)
      for s in site_ids:
        if self._sites[s].is_up():
          self._add_to_txn_sites(transaction, s)
          retval = self._sites[s].read(transaction, var)
          if retval:
            if retval['status'] == 'success':
              return 'Read var %s for txn %s at time_stamp %d, value: %s' \
                % (var, txn_id, self._ts, repr(retval['data']))
              if transaction.is_waiting():
                transaction.activate()
              return
            elif retval['status'] == 'exception':
              args = retval['args']
              return self._handle_wait_die(('read', txn_id, var), args[0], args[1])
      # If we reach here, we weren't able to find a site to read from
      return 'Unable to read %s, no site available' % var
      self._add_to_waitlist(('read', txn_id, var))
    else:
      return 'Transaction %s is in aborted state' % txn_id


  def _handle_wait_die(self, command_tuple, lock_type, conflicting_txns):
    txn_id = command_tuple[1]
    transaction = self._transactions[txn_id]
    if isinstance(conflicting_txns, str):
      conflicting_txn = self._transactions[conflicting_txns]
      if transaction.get_ts() > conflicting_txn.get_ts():
        self._abort(transaction)
        return 'Aborted transaction %s at time_stamp %d' % (txn_id, self._ts)
      else:
        self._add_to_waitlist(command_tuple)
        return 'Waitlisted transaction %s at time_stamp %d' % (txn_id, self._ts)
    elif isinstance(conflicting_txns, list):
      for conflicting_txn_id in conflicting_txns:
        conflicting_txn = self._transactions[conflicting_txn_id]
        if transaction.get_ts() > conflicting_txn.get_ts():
          self._abort(transaction)
          return 'Aborted transaction %s at time_stamp %d' % (txn_id, self._ts)
      self._add_to_waitlist(command_tuple)
      return 'Waitlisted transaction %s at time_stamp %d' % (txn_id, self._ts)


  def _add_to_waitlist(self, command_tuple):
    txn_id = command_tuple[1]
    transaction = self._transactions[txn_id]
    if not transaction.is_waiting():
      print 'Waitlisted Transaction %s at time_stamp %d' % (txn_id,
          self._ts)
      self._wait_list.append(command_tuple)
      transaction.wait()


  def _abort(self, transaction):
    txn_id = transaction.get_id()
    if txn_id in self._txn_sites:
      for s in self._txn_sites[transaction.get_id()]:
        self._sites[s].abort(transaction)
        transaction.abort()
      print 'Aborted Transaction %s at time_stamp %d' % (txn_id, self._ts)
      self._retry_waiting_txns()
    else:
      print 'Txn %s not found on transaction manager' % txn_id


  def _abort_site_txns(self, site_id):
    aborted_txns = []
    for txn_id, txn_sites in self._txn_sites.iteritems():
      if site_id in txn_sites:
        transaction = self._transactions[txn_id]
        self._abort(transaction)
        aborted_txns.append(txn_id)
        continue
    return aborted_txns


  def _retry_waiting_txns(self):
    i = 0
    while i < len(self._wait_list):
      operation = self._wait_list[i]
      txn_id = operation[1]
      transaction = self._transactions[txn_id]
      if operation[0] == 'write':
        var, value = operation[2:]
        self.write(txn_id, var, value)
      elif operation[0] == 'read':
        var = operation[2]
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
            self._abort(transaction)
            return 'One of the site accessed by txn failed, aborting'
        self._retry_waiting_txns()
      return 'Ended txn %s at time_stamp %d' % (txn_id, self._ts)
    else:
      return 'Transaction %s is in aborted state' % txn_id


  def dump_site(self, site_id):
    return self._sites[site_id].dump()

  def dump_var(self, var_id):
    var_index = int(var_id[1:])
    sites = self._get_sites_for_var(var_index)
    sites_var = {}
    for s in sites:
      sites_var[str(s)] = self._sites[s].dump(var_id)
    return sites_var

  def dump_site_var(self, site_id, var_id):
     return self._sites[site_id].dump(var_id)

  def dump_all(self):
    site_var = {}
    for s, obj in self._sites.iteritems():
      site_var[str(s)] = obj.dump()
    return site_var


  def fail(self, site_id):
    if site_id in self._sites:
      self._sites[site_id].fail()
      aborted_txns = self._abort_site_txns(site_id)
      return 'Site %s failed at time_stamp %d, aborted following transactions: %s' \
          % (site_id, self._ts, aborted_txns)
    else:
      return 'Unknown site %s' % site_id


  def recover(self, site_id):
    self._sites[site_id].recover()
    self._retry_waiting_txns()
    return 'Site %s recovered at time_stamp %d' % (site_id, self._ts)

if __name__ == '__main__':
  txn_mgr = TransactionManager()
