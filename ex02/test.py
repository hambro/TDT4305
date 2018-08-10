from part2 import main, multiply_list, get_or_zero
import unittest

# Not a very good unit test, but rather an integration test

class TestDataOutput(unittest.TestCase):    
    test_learning = 'data/testdata.tsv'
    test_input = ['test/1', 'test/2','test/3','test/4']
    test_output = ['test/1out','test/2out','test/3out','test/4out', ]
    test_data_output = [('NewYork', 0.6), ('NewYork',0.2), ('London', 0.025), ('NewYork', 0.133),]

    def test_static_values(self):

        for input_path, output_path, (test_place, test_value) in zip(self.test_input, self.test_output, self.test_data_output):
            main(self.test_learning, input_path, output_path, sample=False)
            
            with open(output_path, 'r') as test_out:
                output = test_out.readline().split('\t')
                place, value = output[:2]

                self.assertTrue(test_place in place)
                self.assertAlmostEqual(test_value, float(value), places = 3)

       
    def test_zero(self):
        main(self.test_learning, 'test/5', 'test/5out', sample=False)   
        with open('test/5out', 'r') as test_out:
            output = test_out.readline().split('\t')
            self.assertEqual(output, ['','',''])          
       
test = TestDataOutput()
test.test_static_values()
test.test_zero()
