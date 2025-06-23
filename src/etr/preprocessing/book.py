import numpy as np
import numba
from typing import List


@numba.njit
def _calc_impact_price(array, target_amount=5e-3, use_term=False, force_return=False):
    prices = np.empty(array.shape[0])
    weights = np.empty(array.shape[0])
    cum_amount = 0.0
    idx = 0
    fill_target_amount = False
    for i in range(array.shape[0]):
        price = array[i, 0]
        qty = array[i, 1]
        delta_amount = price * qty if use_term else qty
        cum_amount += delta_amount
        if cum_amount >= target_amount:
            weights[idx] = target_amount - (cum_amount - delta_amount)
            prices[idx] = price
            idx += 1
            fill_target_amount = True
            break
        else:
            prices[idx] = price
            weights[idx] = delta_amount
            idx += 1

    # null handling
    if idx == 0:
        return np.nan
    if (not fill_target_amount) and (not force_return):
        return np.nan

    return np.average(prices[:idx], weights=weights[:idx])


def calc_impact_price(
    price_amount_arrays: np.ndarray,
    target_amount=1e6,
    use_term=True,
    force_return=False
) -> List[float]:
    """Calculate VWAP price to fill `target amount`.

    Args:
        price_amount_arrays (np.ndarray): ndarray(ndarray([price, amount]))
        target_amount (float):
        use_term (bool): if True, amount is based on term currency otherwise on base currency.
        force_return (bool): Defaults to False.

    Returns:
        (List[float]): impact price array
    """
    return [
        _calc_impact_price(
            np.vstack(array),
            target_amount=target_amount,
            use_term=use_term,
            force_return=force_return
        ) if len(array) > 0 else np.nan
        for array in price_amount_arrays
    ]
