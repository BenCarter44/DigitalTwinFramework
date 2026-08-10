# given array of inputs,
# return the array of outputs according to the dataset


import numpy as np
from tensorflow.keras.datasets import mnist
import matplotlib

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

rng = np.random.default_rng(57)


def do_simulation(target_labels, sample_size):
    # return samples of given label and sample size.

    # Load the dataset
    (train_images, train_labels), (test_images, test_labels) = mnist.load_data()

    # Normalize the images to values between 0 and 1
    train_images = train_images / 255.0
    # test_images = test_images / 255.0

    # Convert labels to one-hot encoded format

    mask = np.isin(train_labels, target_labels)

    target = train_images[mask]
    indices = np.random.choice(len(target), size=sample_size, replace=False)
    sample = target[indices]

    return sample, train_labels[mask][indices]


# Function to plot images and their predictions
def plot_image(i, predictions_array, true_label, img):
    predictions_array, true_label, img = predictions_array[i], true_label[i], img[i]
    plt.grid(False)
    plt.xticks([])
    plt.yticks([])

    plt.imshow(img, cmap=plt.cm.binary)

    predicted_label = np.argmax(predictions_array)
    if predicted_label == true_label:
        color = "blue"
    else:
        color = "red"

    plt.xlabel(f"Predicted: {predicted_label} (True: {true_label})", color=color)


# plt.figure()
# plot_image(0, [1], [1], do_simulation(1, 2))
# plt.show()
