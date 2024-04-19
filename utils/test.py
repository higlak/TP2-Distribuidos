from unittest import TestCase
import unittest

x = '"Philip Nel takes a fascinating look into the key aspects of Seuss\'s career - his poetry, politics, art, marketing, and place in the popular imagination."" ""Nel argues convincingly that Dr. Seuss is one of the most influential poets in America. His nonsense verse, like that of Lewis Carroll and Edward Lear, has changed language itself, giving us new words like ""nerd."" And Seuss\'s famously loopy artistic style - what Nel terms an ""energetic cartoon surrealism"" - has been equally important, inspiring artists like filmmaker Tim Burton and illustrator Lane Smith. --from back cover"'
#Returns a copy of the string with the leading and trailing characters removed.
print(x.strip('"'))