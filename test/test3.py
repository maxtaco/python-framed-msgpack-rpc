import sys
sys.path.append("../")

import time
import random
import unittest
import fmprpc
from fmprpc.pipeliner import Pipeliner

class PipelinerTest(unittest.TestCase):

    def test_pipeliner(self):
        def f(i):
            time.sleep(random.random())
            return i*2
        def launch (p, i):
            p.push(lambda: f(i))
        p = Pipeliner(20)
        p.start()
        for i in range(100):
            launch(p, i)
        res = p.flush()

        for (k,v) in res.items():
            self.assertEqual(k*2, v)

