"""Unit tests for the event-count vectorizer (src/features/vectorizer.py)."""

import numpy as np
import pytest

from src.features.vectorizer import UNSEEN_FEATURE, EventCountVectorizer

TRAIN = [{1: 3, 2: 1}, {1: 2, 3: 1}, {1: 1}]


def test_fit_learns_sorted_vocabulary():
    """TC-15: one column per training template, plus __unseen__."""
    vec = EventCountVectorizer().fit(TRAIN)
    assert vec.template_ids == [1, 2, 3]
    assert vec.feature_names == ["T1", "T2", "T3", UNSEEN_FEATURE]
    assert vec.block_freq == [3, 1, 1]


def test_unknown_templates_go_to_unseen_column():
    """TC-16: templates not in the vocabulary (and UNSEEN=-1) are counted as unseen."""
    vec = EventCountVectorizer().fit(TRAIN)
    row = vec.transform([{1: 2, 99: 3, -1: 1}])[0]
    np.testing.assert_array_equal(row, [2, 0, 0, 4])


def test_min_block_freq_filters_rare_templates():
    """TC-17: templates in fewer blocks than min_block_freq get no column."""
    vec = EventCountVectorizer(min_block_freq=2).fit(TRAIN)
    assert vec.template_ids == [1]


def test_save_and_load_roundtrip(tmp_path):
    """TC-18: a saved vectorizer produces identical features after loading."""
    vec = EventCountVectorizer(use_tfidf=True).fit(TRAIN)
    path = tmp_path / "vec.json"
    vec.save(path)
    loaded = EventCountVectorizer.load(path)
    np.testing.assert_allclose(vec.transform(TRAIN), loaded.transform(TRAIN))


def test_transform_before_fit_raises():
    """TC-19: using an unfitted vectorizer is an error."""
    with pytest.raises(RuntimeError):
        EventCountVectorizer().transform(TRAIN)
