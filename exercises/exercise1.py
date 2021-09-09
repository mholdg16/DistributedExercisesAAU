import random
import time

from emulators.Device import Device
from emulators.Medium import Medium
from emulators.MessageStub import MessageStub


class GossipMessage(MessageStub):

    def __init__(self, sender: int, destination: int, secrets):
        super().__init__(sender, destination)
        # we use a set to keep the "secrets" here
        self.secrets = secrets

    def __str__(self):
        return f'{self.source} -> {self.destination} : {self.secrets}'


class Gossip(Device):

    def __init__(self, index: int, number_of_devices: int, medium: Medium):
        super().__init__(index, number_of_devices, medium)
        # for this exercise we use the index as the "secret", but it could have been a new routing-table (for instance)
        # or sharing of all the public keys in a cryptographic system
        self._secrets = set([index])

    def run(self):
        while True:
            # choose a random receiver (that is not self)
            p = self.index()
            while p == self.index():
                p = random.randint(0, self.number_of_devices() - 1)
            
            while True:
                ingoing = self.medium().receive()

                if ingoing is None:
                    break

                # join the received secrets with known secrets
                self._secrets.update(ingoing.secrets)
            
            self.medium().send(GossipMessage(self.index(), p, self._secrets))

            # the following is your termination condition, but where should it be placed?
            if len(self._secrets) == self.number_of_devices():
                break

            # wait ~[0,1] seconds to avoid livelock
            time.sleep(random.random())


    def print_result(self):
        print(f'\tDevice {self.index()} got secrets: {self._secrets}')


class GossipCircular(Device):

    def __init__(self, index: int, number_of_devices: int, medium: Medium):
        super().__init__(index, number_of_devices, medium)
        # for this exercise we use the index as the "secret", but it could have been a new routing-table (for instance)
        # or sharing of all the public keys in a cryptographic system
        self._secrets = set([index])

    def run(self):
        while True:
            # choose receiver (left or right)
            if random.randint(0,1) == 1:
                p = (self.index() + 1) % self.number_of_devices()
            else:
                p = (self.index() - 1) % self.number_of_devices()
            
            while True:
                ingoing = self.medium().receive()

                if ingoing is None:
                    break

                # join the received secrets with known secrets
                self._secrets.update(ingoing.secrets)
            
            self.medium().send(GossipMessage(self.index(), p, self._secrets))

            # the following is your termination condition, but where should it be placed?
            if len(self._secrets) == self.number_of_devices():
                break

            # wait ~[0,1] seconds to avoid livelock
            time.sleep(random.random())


    def print_result(self):
        print(f'\tDevice {self.index()} got secrets: {self._secrets}')