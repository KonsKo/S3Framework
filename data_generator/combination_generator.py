"""Module to generate combinations for requests."""
from itertools import combinations, product
from typing import Generator, Optional

from data_generator.predictor_rc import HttpCodePredictorByACL


class CombinationGenerator(object):
    """Base class make combinations of sequence(s)."""

    predictor = HttpCodePredictorByACL()

    span_delimiter = '-'

    def __init__(self, source: dict):
        """
        Init class instance.

        Args:
            source (dict): source data to make combinations
        """
        self.source: dict = source
        self.source_mod: list = []

    def make_combinations(self, sequence: dict, span: Optional[str] = None) -> Generator:
        """
        Create combinations of iterable object.

        Args:
            sequence (dict): source to make combinations
            span (Optional[str]): span for size combination

        Yields:
            combination (Generator): one combination of all combinations
        """
        span_start = 1
        span_finish = len(sequence)

        if span:
            span_start, span_finish = self._process_span(span=span)

        for rep in range(span_start, span_finish + 1):

            for comb in combinations(sequence.items(), rep):
                yield {comb_k: comb_v for comb_k, comb_v in comb}

    def make_all_combinations(self, span: Optional[str] = None):
        """
        Make all possible combinations of source sequence (self.source).

        Original source is dictionary and value for every key may be string or list.
        If value is list then those value will participate in combinations.

        Args:
            span (Optional[str]): span for size combination

        Yields:
            combination (Generator): one combination per call
        """
        self._prepare_source()

        for prod in product(*self.source_mod):

            # `product` returns tuple of dict - make it merged dict
            prod = {k_pc: v_pc for part_prod in prod for k_pc, v_pc in part_prod.items()}
            yield from self.make_combinations(prod, span)

    def _prepare_source(self):
        """Prepare source original data to be processed."""
        for source_k, source_v in self.source.items():
            tmp_source_mod = []
            if isinstance(source_v, list):
                for el in source_v:
                    tmp_source_mod.append({source_k: el})
                self.source_mod.append(tmp_source_mod)

            elif isinstance(source_v, str):
                self.source_mod.append(
                    [{source_k: source_v}],
                )

    def _process_span(self, span: str) -> (int, int):
        """
        Process span parameter.

        Args:
            span (str): span to process

        Span can be:
            `1-3` - from 1 to 3
            `3` - only 3
            `2-` - from 2
            `-4` - till 4

        Returns:
            span (tuple[int, int]): start and finish span

        """
        max_l = len(self.source)

        if isinstance(span, str):
            span = span.split(
                self.span_delimiter,
            )

            # case span=`3`
            if len(span) == 1 and span[0].isdigit() and 0 < int(span[0]) <= max_l:
                start = finish = int(span[0])

            else:

                if span[0] and span[0].isdigit() and 0 < int(span[0]) <= max_l:
                    start = int(span[0])

                    # case span=`1-3`
                    if span[1] and span[1].isdigit() and int(span[0]) < int(span[1]) <= max_l:
                        finish = int(span[1])

                    # case span=`1-`
                    else:
                        finish = max_l

                elif not span[0]:
                    start = 1

                    # case span=`-4`
                    if span[1] and span[1].isdigit() and 1 < int(span[1]) <= max_l:
                        finish = int(span[1])
                    else:
                        finish = max_l

                else:
                    raise ValueError('Wrong parameter format.')

        else:
            raise TypeError('Parameter must be string.')

        return start, finish
