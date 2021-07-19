import numpy

a = numpy.array([3, 2, 1, 2, 5, 7, 4, 7, 8])

itemindex = numpy.argwhere(a == 7)

print(itemindex)

print(a)
