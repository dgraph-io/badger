#!/bin/bash


# You will need the flatbuffer compiler (flatc) https://google.github.io/flatbuffers/flatbuffers_guide_building.html

flatc --go flatbuffer.fbs
# Move files to the correct directory.
mv fb/* ./
rmdir fb
