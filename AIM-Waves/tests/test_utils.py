import pytest
from aim_waves.core.utils import normalize_string_for_comparison, parse_recommendation_output

def test_normalize_string():
    assert normalize_string_for_comparison("205/55 R16") == "20555R16"
    assert normalize_string_for_comparison("volkswagen golf") == "VOLKSWAGENGOLF"
    assert normalize_string_for_comparison(None) == ""

def test_parse_output_valid():
    # Valid 22-token string
    raw = "VW GOLF 20555R16 111 222 333 444 " + " ".join([str(i) for i in range(5, 21)])
    veh, size, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw)
    
    assert veh == "VW GOLF"
    assert size == "20555R16"
    assert hb1 == "111"
    assert len(skus) == 16
    assert skus[0] == "5"

def test_parse_output_invalid():
    # Too short
    raw = "VW GOLF 20555R16 111"
    veh, size, hb1, hb2, hb3, hb4, skus = parse_recommendation_output(raw)
    assert veh == "ERROR_VEHICLE"
    assert skus[0] == "FormatError"
