class Lock:
  READ  = 0
  WRITE = 1

class AcquireLockException(Exception):
  def __init__(self, *args):
    self.args = [a for a in args]


class LockManager:
  def __init__(self, site_variables):
    self._init_lock_tables()


  def _init_lock_tables(self):
    for s in site_variables:
      self._write_lock_table[s] = None
      self._read_lock_table[s]  = []


  def acquire_read_lock(self, transaction, variable):
    if variable in self._write_lock_table:
      if self._write_lock_table[variable] != transaction.get_id():
        raise AcquireLockException(Lock.READ, self._write_lock_table[variable])
        return
    self._read_lock_table[variable].append(transaction.get_id())


  def acquire_write_lock(self, transaction, variable):
    if variable in self._write_lock_table:
      if self._write_lock_table[variable] != transaction.get_id():
        raise AcquireLockException(Lock.WRITE, self._write_lock_table[variable])
        return
    elif variable in self._read_lock_table:
      for txn in self._read_lock_table[variable]:
        if txn != transaction.get_id():
          raise AcquireLockException(Lock.READ, self._read_lock_table[variable])
          return
    self._write_lock_table[variable] = transaction.get_id()


  def release_read_lock(self, transaction, variable):
    if variable in self._read_lock_table:
      try:
        self._read_lock_table[variable].remove(transaction.get_id())
      except ValueError:
        print 'This transaction %s didn\'t have a read lock on the variable' \
            % transaction.get_id()
    else:
      print 'There is no read lock on variable %s' % variable


  def release_write_lock(self, transaction, variable):
    if variable in self._write_lock_table:
      if self._write_lock_table[variable] == transaction.get_id():
        self._write_lock_table[variable] = None
    else:
      print 'There is no write lock on variable %s' % variable


  def release_all_locks(self, transaction=None):
    if not transaction:
      self._init_lock_tables()
    else:
      self._release_write_locks(transaction)
      self._release_read_locks(transaction)


  def _release_write_locks(self, transaction):
    for variable in self._write_lock_table.keys():
      if self._write_lock_table[variable] == transaction.get_id():
        self._write_lock_table[variable] = None


  def _release_read_locks(self, transaction):
    for variable in self._read_lock_table.keys():
      try:
         self._read_lock_table[variable].remove(transaction.get_id())
      except ValueError:
        continue
