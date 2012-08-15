#!/usr/bin/env python
# (C) Copyright 2010 Brandyn A. White
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

"""Hadoopy Face Finding Demo"""

__author__ =  'Brandyn A. White <bwhite@cs.umd.edu>'
__license__ = 'GPL V3'

import hadoopy
import Image
import imfeat
import cStringIO as StringIO
import os
import cv2


def _detect_faces(img, cascade):
    min_size = (20, 20)
    image_scale = 2
    haar_scale = 1.2
    min_neighbors = 2
    haar_flags = 0
    small_width = img.shape[1] / image_scale
    small_height = img.shape[0] / image_scale
    small_img = cv2.resize(img, (small_width, small_height))
    cv2.equalizeHist(small_img, small_img)
    faces = cascade.detectMultiScale(small_img, scaleFactor=haar_scale, minNeighbors=min_neighbors, flags=haar_flags, minSize=min_size)
    return [((x * image_scale, y * image_scale,
              w * image_scale, h * image_scale), n)
            for (x, y, w, h), n in faces]


class Mapper(object):

    def __init__(self):
        path = 'haarcascade_frontalface_default.xml'
        if os.path.exists(path):
            self._cascade = cv2.CascadeClassifier(path)
        else:
            path = ('fixtures/haarcascade_frontalface_default.xml')
            if os.path.exists(path):
                self._cascade = cv2.CascadeClassifier(path)
            else:
                raise ValueError("Can't find .xml file!")

    def map(self, key, value):
        """
        Args:
            key: Image name
            value: Image as jpeg byte data

        Yields:
            A tuple in the form of (key, value)
            key: Image name
            value: (image, faces) where image is the input value and faces is
                a list of ((x, y, w, h), n)
        """
        try:
            image = imfeat.image_fromstring(value)
        except:
            hadoopy.counter('DATA_ERRORS', 'ImageLoadError')
            return
        faces = _detect_faces(image)
        if faces:
            yield key, (value, faces)


if __name__ == "__main__":
    hadoopy.run(Mapper, required_files=['haarcascade_frontalface_default.xml'])
