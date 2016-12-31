
import time

class RunDurationClock(object):

    def __init__(self, name='', print_time=True):
        self.print_time = print_time
        self.name = name

    def __enter__(self):
        self.begin()

    def begin(self):
        self.begin_time = time.clock()

    def __exit__(self, unused_type, unused_value, unused_traceback):
        self.end()
        if self.print_time:
            print '\nTask name: %s\nRun duration in seconds: %f\n' % (self.name, self.end_time)

    @property
    def current_time(self):
        return time.clock() - self.begin_time

    @property
    def run_duration(self):
        return self.end_time - self.begin_time

    def end(self):
        self.end_time = time.clock() - self.begin_time
        return self.end_time

def usage_example():

    with RunDurationClock("Test task"):
        print "testing RunDurationClock"
        lst = []
        for i in range(10000000): lst.append("x")


if __name__ == "__main__":
    usage_example()