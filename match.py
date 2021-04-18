from collections import namedtuple
from itertools import groupby
from typing import List
from database import db_get, bids_update
from loguru import logger

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y


Bid = namedtuple("Bid", ["id", "action", "value", "price", "bidder"])


# Given three colinear points p, q, r, the function checks if
# point q lies on line segment 'pr'
def onSegment(p, q, r):
    if (
        (q.x <= max(p.x, r.x))
        and (q.x >= min(p.x, r.x))
        and (q.y <= max(p.y, r.y))
        and (q.y >= min(p.y, r.y))
    ):
        return True
    return False


def orientation(p, q, r):
    # to find the orientation of an ordered triplet (p,q,r)
    # function returns the following values:
    # 0 : Colinear points
    # 1 : Clockwise points
    # 2 : Counterclockwise

    # See https://www.geeksforgeeks.org/orientation-3-ordered-points/amp/
    # for details of below formula.

    val = (float(q.y - p.y) * (r.x - q.x)) - (float(q.x - p.x) * (r.y - q.y))
    if val > 0:

        # Clockwise orientation
        return 1
    elif val < 0:

        # Counterclockwise orientation
        return 2
    else:

        # Colinear orientation
        return 0


# The main function that returns true if
# the line segment 'p1q1' and 'p2q2' intersect.
def doIntersect(p1, q1, p2, q2):

    # Find the 4 orientations required for
    # the general and special cases
    o1 = orientation(p1, q1, p2)
    o2 = orientation(p1, q1, q2)
    o3 = orientation(p2, q2, p1)
    o4 = orientation(p2, q2, q1)

    # General case
    if (o1 != o2) and (o3 != o4):
        return True

    # Special Cases

    # p1 , q1 and p2 are colinear and p2 lies on segment p1q1
    if (o1 == 0) and onSegment(p1, p2, q1):
        return True

    # p1 , q1 and q2 are colinear and q2 lies on segment p1q1
    if (o2 == 0) and onSegment(p1, q2, q1):
        return True

    # p2 , q2 and p1 are colinear and p1 lies on segment p2q2
    if (o3 == 0) and onSegment(p2, p1, q2):
        return True

    # p2 , q2 and q1 are colinear and q1 lies on segment p2q2
    if (o4 == 0) and onSegment(p2, q1, q2):
        return True

    # If none of the cases
    return False


class MatchMaker:
    Bid = namedtuple("Bid", ["id", "action", "value", "price", "bidder"])

    def match(self, buys: List, sells: List) -> (float, float):
        """
        buys - [(value1, price1, bidder1), (value2, price2, bidder2), ...]
        sells - [(value1, price1, bidder1), (value2, price2, bidder2), ...]
        """

        is_matched, ret = self._get_matched_point(buys, sells)
        if is_matched:
            matched_price, matched_value, thresh_buy, thresh_sell = ret
            print(matched_price, matched_value)
            return is_matched, self._distribute_matchresult(buys, sells, ret)
        return is_matched, None

    def _sort_bids(self, buys: List, sells: List) -> (List, List):
        """
        buys: sort price from high to low
        sells: sort price from low to high
        """

        sorted_buys = sorted(buys, key=lambda bid: bid.price, reverse=True)
        sorted_sells = sorted(sells, key=lambda bid: bid.price)
        return sorted_buys, sorted_sells

    def _accumulate_bids(self, sorted_buys: List, sorted_sells: List) -> (List, List):
        """
        Accumulate buys and sells volume value for demand-supply list
        """
        Bid = namedtuple('Bid', ['id', 'action', 'value', 'price', 'bidder'])
        accumulate_buys, accumulate_sells = (
            [
                Bid(
                    id=bid.id,
                    action=bid.action,
                    price=bid.price,
                    bidder=bid.bidder,
                    value=sum(sum_bid.value for sum_bid in bids[: i + 1]),
                )
                for i, bid in enumerate(bids)
            ]
            for bids in (sorted_buys, sorted_sells)
        )

        return accumulate_buys, accumulate_sells

    def _get_base_values(self, buys: List, sells: List) -> (List, List):
        """
        Get the base values for all the buys and sells. (unify x axis)
        """

        base_value = {
            *{buy.value for buy in buys},
            *{sell.value for sell in sells},
        }
        base_value = list(base_value)
        base_value.sort()
        return base_value

    def _get_matched_point(self, buys: List, sells: List) -> (float, float):
        sorted_buys, sorted_sells = self._sort_bids(buys, sells)
        accumulate_buys, accumulate_sells = self._accumulate_bids(
            sorted_buys, sorted_sells
        )
        base_values = self._get_base_values(accumulate_buys, accumulate_sells)

        new_buys = [Bid(0, "buy", 0, accumulate_buys[0].price, "")]
        new_sells = [Bid(0, "sell", 0, accumulate_sells[0].price, "")]
        accumulate_buys = [Bid(0, "buy", 0, accumulate_buys[0].price, "")] + accumulate_buys
        accumulate_sells = [Bid(0, "sell", 0, accumulate_sells[0].price, "")] + accumulate_sells

        matched_price = 0
        matched_value = 0
        for value in base_values:
            # price buy
            for i in range(1, len(accumulate_buys)):
                if accumulate_buys[i - 1].value < value <= accumulate_buys[i].value:
                    new_buys.append(Bid(accumulate_buys[i].id, "buy", value, accumulate_buys[i].price, ""))
                    break
            else:
                new_buys.append(Bid(accumulate_buys[0].id, "buy", value, accumulate_buys[0].price, ""))

            # value sell
            for i in range(1, len(accumulate_sells)):
                if accumulate_sells[i - 1].value < value <= accumulate_sells[i].value:
                    new_sells.append(Bid(accumulate_sells[i].id, "sell", value, accumulate_sells[i].price, ""))
                    break
            else:
                new_sells.append(Bid(accumulate_sells[-1].id, "sell", value, accumulate_sells[-1].price, ""))


        for i in range(1, len(new_buys)):
            # print(new_buys[i], new_sells[i])
            b1 = Point(*new_buys[i - 1][2:4])
            b2 = Point(*new_buys[i][2:4])
            s1 = Point(*new_sells[i - 1][2:4])
            s2 = Point(*new_sells[i][2:4])
            buy_line = (b1, b2)
            sell_line = (s1, s2)
            if doIntersect(*buy_line, *sell_line):
                matched_price = b2.y
                matched_value = b2.x
                thresh_buy = b2.y
                thresh_sell = s2.y
                return True, (matched_price, matched_value, thresh_buy, thresh_sell)

        return False, None

    def _distribute_matchresult(
        self, buys: List, sells: List, ret: tuple
    ) -> (List, List):
        # Bid = namedtuple('Bid', ['value', 'price', 'bidder'])
        win_price, win_value, thresh_buy, thresh_sell = ret

        buy_winners = [bid for bid in buys if bid.price >= thresh_buy]
        sell_winners = [bid for bid in sells if bid.price <= thresh_sell]

        buy_matchresult = []
        buy_wins = {
            key: list(group) for key, group in groupby(buy_winners, lambda x: x.price)
        }

        buy_pool = win_value
        for key in sorted(buy_wins.keys(), reverse=True):
            winners = buy_wins[key]
            winners_subtotal = sum([bid.value for bid in winners])
            if buy_pool > 0:
                if winners_subtotal <= buy_pool:
                    buy_matchresult.extend(
                        [
                            Bid(id=bid.id, action=bid.action, value=bid.value, price=win_price, bidder=bid.bidder)
                            for bid in winners
                        ]
                    )
                else:
                    buy_matchresult.extend(
                        [
                            Bid(
                                id=bid.id,
                                action=bid.action,
                                value=(bid.value / winners_subtotal) * buy_pool,
                                price=win_price,
                                bidder=bid.bidder,
                            )
                            for bid in winners
                        ]
                    )
                buy_pool -= winners_subtotal

        sell_matchresult = []
        sell_wins = {
            key: list(group) for key, group in groupby(sell_winners, lambda x: x.price)
        }

        sell_pool = win_value
        for key in sorted(sell_wins.keys()):
            winners = sell_wins[key]
            winners_subtotal = sum([bid.value for bid in winners])
            if sell_pool > 0:
                if winners_subtotal <= sell_pool:
                    sell_matchresult.extend(
                        [
                            Bid(id=bid.id, action=bid.action, value=bid.value, price=win_price, bidder=bid.bidder)
                            for bid in winners
                        ]
                    )
                else:
                    sell_matchresult.extend(
                        [
                            Bid(
                                id=bid.id,
                                action=bid.action,
                                value=(bid.value / winners_subtotal) * sell_pool,
                                price=win_price,
                                bidder=bid.bidder,
                            )
                            for bid in winners
                        ]
                    )
                sell_pool -= winners_subtotal

        return buy_matchresult, sell_matchresult



def match(match_time, flag):
    # sell [20, 5, 10, 15, 20], [20, 25, 40, 60, 60]);
    # buy [10, 30, 20, 15, 10], [70, 55, 30, 25, 10]);

    data = db_get("bids", time=match_time, flag=flag)

    buys, sells = ([
            Bid(id=row.bid, action=row.action, value=row.target_volume, price=row.target_price, bidder=row.bidder)
            for index, row in data[data["action"] == action].iterrows()
        ]
        for action in ["buy", "sell"]
    )

    win_buys, win_sells = list(), list()
    if buys and sells:
        has_result, ret = MatchMaker().match(buys, sells)
        if ret:
            win_buys, win_sells = ret
            logger.info(f"win_buys: {win_buys}")
            logger.info(f"win_sells: {win_sells}")

    bids_update(match_time, flag, win_buys, win_sells)
