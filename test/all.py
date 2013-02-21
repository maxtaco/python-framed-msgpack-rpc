
import unittest
import os
import os.path

def main():
    loader = unittest.TestLoader()
    wd = os.path.dirname(__file__)
    suite = loader.discover(start_dir=wd)
    unittest.TextTestRunner().run(suite)

if __name__ == "__main__":
    main()
