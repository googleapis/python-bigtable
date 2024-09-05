import ast
import sys
import os
import black
import pytest
import yaml
# add cross_sync to path
sys.path.append("google/cloud/bigtable/data/_sync/cross_sync")
from transformers import SymbolReplacer, AsyncToSync, RmAioFunctions, CrossSyncClassDecoratorHandler, CrossSyncClassDecoratorHandler


def loader():
    dir_name = os.path.join(os.path.dirname(__file__), "test_cases")
    for file_name in os.listdir(dir_name):
        if not file_name.endswith(".yaml"):
            print(f"Skipping {file_name}")
            continue
        test_case_file = os.path.join(dir_name, file_name)
        # load test cases
        with open(test_case_file) as f:
            print(f"Loading test cases from {test_case_file}")
            test_cases = yaml.safe_load(f)
            for test in test_cases["tests"]:
                test["file_name"] = file_name
                yield test

@pytest.mark.parametrize(
    "test_dict", loader(), ids=lambda x: f"{x['file_name']}: {x.get('description', '')}"
)
def test_e2e_scenario(test_dict):
    before_ast = ast.parse(test_dict["before"]).body[0]
    transformers = [globals()[t] for t in test_dict["transformers"]]
    got_ast = before_ast
    for transformer in transformers:
        got_ast = transformer().visit(got_ast)
    final_str = black.format_str(ast.unparse(got_ast), mode=black.FileMode())
    expected_str = black.format_str(test_dict["after"], mode=black.FileMode())
    assert final_str == expected_str, f"Expected:\n{expected_str}\nGot:\n{final_str}"
