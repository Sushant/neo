import sys
import traceback
import xmlrpclib
from pprint import pprint

from transaction_manager import TransactionManager

TXN_MGR_PORT = 7777

class Neo:
  def __init__(self, filename):
    self._fp = open(filename, 'r')
    self._tm = xmlrpclib.ServerProxy('http://localhost:%d' % TXN_MGR_PORT,
        allow_none=True)
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
    print '---------------------------------------------------------\n'
    print 'begin: ', txn_id
    try:
      print self._tm.begin(txn_id.strip())
    except Exception as e:
      print traceback.format_exc()

  def beginRO(self, txn_id):
    print '---------------------------------------------------------\n'
    print 'beginRO: ', txn_id
    try:
      print self._tm.beginRO(txn_id.strip())
    except Exception as e:
      print traceback.format_exc()

  def W(self, txn_id, var, value):
    print '---------------------------------------------------------\n'
    print 'W: ', txn_id, var, value
    try:
      print self._tm.write(txn_id.strip(), var.strip(), value.strip())
    except Exception as e:
      print traceback.format_exc()

  def R(self, txn_id, var):
    print '---------------------------------------------------------\n'
    print 'R: ', txn_id, var
    try:
      print self._tm.read(txn_id.strip(), var.strip())
    except Exception as e:
      print traceback.format_exc()

  def end(self, txn_id):
    print '---------------------------------------------------------\n'
    print 'end: ', txn_id
    print self._tm.end(txn_id.strip())

  def dump(self, arg):
    print '---------------------------------------------------------\n'
    print 'dump: ', arg
    if arg:
      try:
        site_id = int(arg.strip())
        pprint(self._tm.dump_site(site_id))
      except ValueError:
        pprint(self._tm.dump_var(arg.strip()))
    else:
      pprint(self._tm.dump_all())

  def fail(self, site_id):
    print '---------------------------------------------------------\n'
    print 'fail: ', site_id
    print self._tm.fail(int(site_id.strip()))

  def recover(self, site_id):
    print '---------------------------------------------------------\n'
    print 'recover: ', site_id
    print self._tm.recover(int(site_id.strip()))


if __name__ == '__main__':
  if len(sys.argv) != 2:
      print 'USAGE: python %s <filename>' % (sys.argv[0])
      sys.exit(1)
  filename = sys.argv[1]
  neo = Neo(filename)
