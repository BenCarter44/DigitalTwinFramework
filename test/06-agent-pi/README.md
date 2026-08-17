

# The internal-producer example

This demo has no external sensor and no `sensor.py`: nothing outside
measures anything.  Its input is a timer, i.e. a producer which belongs to
the twin itself, so it stays a persistent component publishing through
`RuntimeAPI.stream`.  Demos whose input comes from outside (01, 04) bind a
shared channel with `runtime.add_input()` instead and run their sensor as
a separate process.

# Overview: calculating pi:

1. Gregory-Leibniz series:
   start at n = 1
   pi = sum ( (4 / (n*2 - 1)) * (-1)^(n - 1)

2. Monte Carlo
   throw darts, uniform -1 to 1 in XY
   pi = 4 * (distance < 1  / all points)

3. Nilakantha series
  start at n = 1
  pi = 3 + sum (  4 / ( (2n)(2n+1)(2n+2) )     * (-1)^(n-1))



## Steps:
1. Timer sensor, emits signal every second

2. One Agent: PiCalc:

    Three model investigators:
    - Gregory-Leibniz
    - Monte Carlo
    - Nilakantha


3. Downstream sink



## PiCalc - Model Investigators:
- Gregory-Leibniz:
  - on inference, return Pi
  - on training, subscribe to ON_INPUT. Calc series. Push new model

- Nilakantha series:
  - on inference, return Pi
  - on training, subscribe to ON_INPUT. Calc series. Push new model

- Monte Carlo:
  - on inference, return Pi
  - on training:
    - run simulation (throw a dart)
    - run training (calc pi)
    - push model
