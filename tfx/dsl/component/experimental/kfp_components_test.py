from absl.testing import absltest

from tfx.components.base import base_component
from tfx.dsl.component.experimental import kfp_components


def filter_text(text_uri, filtered_text_uri, pattern):
  """Filters the text keeping the lines that contain the pattern."""
  import re
  import tensorflow
  with tensorflow.io.gfile.GFile(text_uri, 'r') as source:
    with tensorflow.io.gfile.GFile(filtered_text_uri, 'w') as dest:
      for line in source:
        if re.search(pattern, line):
          dest.write(line)


class FunctionParserTest(absltest.TestCase):

  def testEnableKfpComponents(self):
    try:
      from kfp import components
    except ImportError:
      self.skipTest('The kfp package is not installed')
    
    kfp_components.enable_kfp_components()

    filter_text_op = components.create_component_from_func(
        func=filter_text,
        base_image='tensorflow/tensorflow-2.2.0',
    )

    component_instance = filter_text_op(
        source_uri='gs://bucket/text.txt',
        filtered_text_uri='gs://bucket/filtered.txt',
        pattern='secret',
    )

    self.assertTrue(isinstance(component_instance, base_component.BaseComponent))
    self.assertEqual(component_instance.executor_spec.image, 'tensorflow/tensorflow-2.2.0')
