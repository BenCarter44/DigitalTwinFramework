import numpy as np
import tensorflow as tf
from tensorflow.keras.datasets import mnist
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Flatten
from tensorflow.keras.utils import to_categorical

rng = np.random.default_rng(57)


def do_train(train_images, train_labels, version_no):

    # Convert labels to one-hot encoded format
    # labels are all the same: length

    train_labels = to_categorical(train_labels, num_classes=10)

    # Create the neural network model
    model = Sequential(
        [
            Flatten(input_shape=(28, 28)),  # Flatten the 28x28 images into a 1D array
            Dense(128, activation="relu"),  # Fully connected layer with 128 neurons
            Dropout(0.1),  # 10% dropout
            Dense(10, activation="softmax"),  # Output layer for 10 classes (digits 0-9)
        ]
    )

    # Compile the model
    model.compile(
        optimizer="adam", loss="categorical_crossentropy", metrics=["accuracy"]
    )

    # Train the model
    model.fit(train_images, train_labels, epochs=5, batch_size=32)

    # Save the model if needed
    model.save(f"handwriting/mnist_model.v{version_no}.keras")
    return f"handwriting/minst_model.keras.v{version_no}"


if __name__ == "__main__":
    from sim import do_simulation

    sample, labels = do_simulation([0], 256)
    do_train(sample, labels, 0)
