#!/usr/bin/env python
# (C) Copyright 2011 Brandyn A. White
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Hadoopy Demo: Find an image in a Haystack (nearest neighbor)"""

__author__ = 'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import hadoopy
import imfeat
import cv2
import numpy as np
from haystack import Reducer


class Mapper(object):

    def __init__(self):
        path = 'target.jpg'
        self.target_image = np.asfarray(imfeat.resize_image(cv2.imread(path), 100, 100))
        self.min_dist = float('inf')
        self.min_key = None
        self.min_value = None

    def map(self, key, value):
        """
        Args:
            key: Image name
            value: Image as jpeg byte data

        Yields:
            A tuple in the form of (key, value)
            key: Constant dummy value
            value: (l2sqr_dist, value)
        """
        try:
            image = imfeat.resize_image(imfeat.image_fromstring(value, {'type': 'numpy', 'mode': 'bgr', 'dtype': 'uint8'}),
                                        100, 100)
        except:
            hadoopy.counter('DATA_ERRORS', 'ImageLoadError')
            return

        # Distance metric
        diff = image - self.target_image
        dist = np.sum(diff * diff)

        # Output
        if dist < self.min_dist:
            self.min_dist = dist
            self.min_key = key
            self.min_value = value

    def close(self):
        yield '', (self.min_dist, self.min_key, self.min_value)


if __name__ == "__main__":
    hadoopy.run(Mapper, Reducer, doc=__doc__)
