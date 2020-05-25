from typing import Dict
import warnings

from tfx.dsl.component.experimental import container_component as tfx_container_component
from tfx.dsl.component.experimental import placeholders
from tfx.types import artifact_utils


def _create_tfx_component_instance(
    component_spec,
    arguments: Dict[str, Any],
    **kwargs,
):
  from kfp import components

  # Replacing the placeholders for input and output paths with TFX placeholders
  def input_path_generator(input_name):
      return placeholders.InputUriPlaceholder(input_name=input_name)

  def output_path_generator(output_name):
      return placeholders.InputUriPlaceholder(output_name=output_name)

  # Resolving the command-line
  resolved_cmd = components._components._resolve_command_line_and_paths(
      component_spec=component_spec,
      arguments=arguments,
      input_path_generator=input_path_generator,
      output_path_generator=output_path_generator,
  )
  if resolved_cmd.input_paths or resolved_cmd.output_paths:
    warnings.warn(
      'TFX does not have support for data passing yet.'
      'The component will most likely fail at runtime.'
    )

  tfx_input_specs = {}
  tfx_output_specs = {}

  for input_spec in component_spec.inputs or []:
    atrifact_class = _type_spec_to_artifact_class(input_spec.type)
    if input_spec.name not in resolved_cmd.inputs_consumed_by_value:
      tfx_input_specs[input_spec.name] = atrifact_class

  for output_spec in component_spec.outputs or []:
      atrifact_class = _type_spec_to_artifact_class(output_spec.type)
      tfx_input_specs[output_spec.name] = atrifact_class

  tfx_component = tfx_container_component.create_container_component(
      name=component_spec.name,
      image=component_spec.implementation.container.image,
      inputs=tfx_input_specs,
      outputs=tfx_output_specs,
      command=resolved_cmd.command + resolved_cmd.args,
  )
  tfx_component_instance = tfx_component(arguments)
  return tfx_component_instance


def enable_kfp_components():
    from kfp import components
    components._components._container_task_constructor = _create_tfx_component_instance

def _type_spec_to_artifact_class(type_spec):
  return artifact_utils.get_artifact_type_class(str(type_spec) or 'Any')