import pytest

pytestmark = [pytest.mark.acceptance, pytest.mark.state_machine]


def test_it_executes_successfully_for_deletion_queue(state_machine, sf_client, del_queue_item):
    pass


def test_it_skips_empty_deletion_queue(state_machine, sf_client):
    pass


def test_it_only_permits_single_executions(state_machine, sf_client, del_queue_item):
    pass


def test_it_errors_for_non_existing_configuration(state_machine, sf_client):
    pass
