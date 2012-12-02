import sys
import traceback
from pprint import pprint

from transaction_manager import TransactionManager

class Neo:
  def __init__(self, filename):
    self._fp = open(filename, 'r')
    self._tm = TransactionManager()
    self._run()


  def _run(self):
    for line in self._fp.readlines():
      line = line.strip()
      if line:
        self._tm.inc_ts()
        commands = line.split(';')
        for c in commands:
          clist = c.strip().strip(')')
          clist = clist.split('(')
          method = clist[0]
          args = clist[1].split(',')
          try:
            _method = getattr(self, method)
            _method(*args)
          except Exception as e:
            print 'Unknown method, exiting Neo...', traceback.format_exc()
            sys.exit(1)

  def begin(self, txn_id):
    print 'begin: ', txn_id
    try:
      self._tm.begin(txn_id)
    except Exception as e:
      print traceback.format_exc()

  def beginRO(self, txn_id):
    print 'beginRO: ', txn_id
    try:
      self._tm.beginRO(txn_id)
    except Exception as e:
      print traceback.format_exc()

  def W(self, txn_id, var, value):
    print 'W: ', txn_id, var, value
    try:
      self._tm.write(txn_id, var, value)
    except Exception as e:
      print traceback.format_exc()

  def R(self, txn_id, var):
    print 'R: ', txn_id, var
    try:
      self._tm.read(txn_id, var)
    except Exception as e:
      print traceback.format_exc()

  def end(self, txn_id):
    print 'end: ', txn_id
    self._tm.end(txn_id)

  def dump(self, arg):
    if arg:
      try:
        site_id = int(arg)
        pprint(self._tm.dump(site_id=site_id))
      except ValueError:
        pprint(self._tm.dump(var_id=arg))
    else:
      pprint(self._tm.dump())
      print 'dump: ', arg

  def fail(self, site_id):
    print 'fail: ', site_id

  def recover(self, site_id):
    print 'recover: ', site_id


if __name__ == '__main__':
  if len(sys.argv) != 2:
      print 'USAGE: python %s <filename>' % (sys.argv[0])
      sys.exit(1)
  filename = sys.argv[1]
  neo = Neo(filename)
