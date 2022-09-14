import numpy as np

original_data = np.array([23, 44, 55, 19, 500, 201]) # Some random numbers to represent the original data to be shuffled
data_length = original_data.shape[0]

# Here we create an array of shuffled indices
shuf_order = np.arange(data_length)
np.random.shuffle(shuf_order)

shuffled_data = original_data[shuf_order] # Shuffle the original data
shuffled_data[0] = 0

# Create an inverse of the shuffled index array (to reverse the shuffling operation, or to "unshuffle")
unshuf_order = np.zeros_like(shuf_order)
unshuf_order[shuf_order] = np.arange(data_length)

unshuffled_data = shuffled_data[unshuf_order] # Unshuffle the shuffled data

print(f"original_data: {original_data}")
print(f"shuffled_data: {shuffled_data}")
print(f"unshuffled_data: {unshuffled_data}")