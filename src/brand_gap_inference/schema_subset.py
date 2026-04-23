from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationIssue:
    path: str
    message: str


def validate_instance(instance: object, schema: dict, path: str = "$") -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    if "type" in schema:
        allowed_types = schema["type"]
        if not isinstance(allowed_types, list):
            allowed_types = [allowed_types]
        if not any(_matches_type(instance, allowed_type) for allowed_type in allowed_types):
            expected = ", ".join(str(allowed_type) for allowed_type in allowed_types)
            issues.append(ValidationIssue(path, f"expected type {expected}"))
            return issues

    if "const" in schema and instance != schema["const"]:
        issues.append(ValidationIssue(path, f"expected constant {schema['const']!r}"))

    if "enum" in schema and instance not in schema["enum"]:
        issues.append(ValidationIssue(path, f"value {instance!r} is not in enum"))

    schema_type = schema.get("type")
    primary_type = schema_type[0] if isinstance(schema_type, list) else schema_type

    if primary_type == "object" and isinstance(instance, dict):
        required_fields = schema.get("required", [])
        for field_name in required_fields:
            if field_name not in instance:
                issues.append(ValidationIssue(f"{path}.{field_name}", "missing required field"))

        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            for field_name in instance:
                if field_name not in properties:
                    issues.append(ValidationIssue(f"{path}.{field_name}", "additional property is not allowed"))

        for field_name, property_schema in properties.items():
            if field_name in instance:
                issues.extend(validate_instance(instance[field_name], property_schema, f"{path}.{field_name}"))

    if primary_type == "array" and isinstance(instance, list):
        min_items = schema.get("minItems")
        if min_items is not None and len(instance) < min_items:
            issues.append(ValidationIssue(path, f"expected at least {min_items} items"))

        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(instance):
                issues.extend(validate_instance(item, item_schema, f"{path}[{index}]"))

    if primary_type == "string" and isinstance(instance, str):
        min_length = schema.get("minLength")
        if min_length is not None and len(instance) < min_length:
            issues.append(ValidationIssue(path, f"expected minimum length {min_length}"))

    if primary_type in {"number", "integer"} and _matches_numeric_type(instance, primary_type):
        minimum = schema.get("minimum")
        if minimum is not None and instance < minimum:
            issues.append(ValidationIssue(path, f"expected minimum value {minimum}"))
        maximum = schema.get("maximum")
        if maximum is not None and instance > maximum:
            issues.append(ValidationIssue(path, f"expected maximum value {maximum}"))

    return issues


def _matches_type(instance: object, expected_type: str) -> bool:
    if expected_type == "object":
        return isinstance(instance, dict)
    if expected_type == "array":
        return isinstance(instance, list)
    if expected_type == "string":
        return isinstance(instance, str)
    if expected_type == "number":
        return _matches_numeric_type(instance, expected_type)
    if expected_type == "integer":
        return _matches_numeric_type(instance, expected_type)
    if expected_type == "boolean":
        return isinstance(instance, bool)
    if expected_type == "null":
        return instance is None
    return False


def _matches_numeric_type(instance: object, expected_type: str) -> bool:
    if isinstance(instance, bool):
        return False
    if expected_type == "number":
        return isinstance(instance, (int, float))
    if expected_type == "integer":
        return isinstance(instance, int)
    return False
