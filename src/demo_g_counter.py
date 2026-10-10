import json

from crdts.g_counter import GCounter


alice = GCounter("alice")
bob = GCounter("bob")

# Both replicas make independent changes.
alice.increment(2)
bob.increment(3)

print("Before synchronisation:")
print("Alice:", alice.state, "Total:", alice.value)
print("Bob:  ", bob.state, "Total:", bob.value)

# Simulate Bob sending his state to Alice as a JSON message.
message_to_alice = json.dumps(bob.state)
alice.merge_state(json.loads(message_to_alice))

# Simulate Alice sending her merged state back to Bob.
message_to_bob = json.dumps(alice.state)
bob.merge_state(json.loads(message_to_bob))

print("\nAfter synchronisation:")
print("Alice:", alice.state, "Total:", alice.value)
print("Bob:  ", bob.state, "Total:", bob.value)

assert alice.state == bob.state
assert alice.value == bob.value == 5

print("\nSUCCESS: Both replicas have converged!")