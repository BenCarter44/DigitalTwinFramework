import numpy as np
import tensorflow as tf
from tensorflow.keras.datasets import mnist
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Flatten
from tensorflow.keras.utils import to_categorical


def do_active(target_labels, sample_size, version_no, *args):

    # Load the dataset
    _, (test_images, test_labels) = mnist.load_data()

    # Normalize the images to values between 0 and 1
    test_images = test_images / 255.0

    # Convert labels to one-hot encoded format

    mask = np.isin(test_labels, target_labels)
    target = test_images[mask]
    indices = np.random.choice(len(target), size=sample_size, replace=False)
    sample = target[indices]
    label_sample = test_labels[mask][indices]

    # Convert labels to one-hot encoded format
    test_labels = to_categorical(label_sample, num_classes=10)

    model = tf.keras.models.load_model(f"handwriting/mnist_model.v{version_no}.keras")

    # Evaluate the model
    loss, acc = model.evaluate(sample, test_labels)

    if acc > 0.95:
        # good!
        return True

    return acc > 0.95
