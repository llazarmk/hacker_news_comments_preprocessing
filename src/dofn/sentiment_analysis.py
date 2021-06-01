from typing import Dict, Iterable

import apache_beam as beam
from nlp.nlp_nltk import SentimentNltk

class SentimentAnalyser(beam.DoFn):

    def __init__(self, feature_columns: dict, batch_size: int = 1000):
        self.feature_columns = feature_columns
        self.batch_size = batch_size
        self.sentiment_analyzer = None

    def nltk_analyzer(self, elements: list, column):
        # {{'neg': 0.0, 'neu': 0.686, 'pos': 0.314, 'compound': 0.987}}
        outputs = elements.copy()
        for output in outputs:
            text = output[column]
            analyzer = self.sentiment_analyzer.process(text)
            output['neg'] = analyzer['neg']
            output['pos'] = analyzer['pos']
            output['neu'] = analyzer['neu']
            output['compound'] = analyzer['compound']

        return outputs

    def process(self, element: Dict, *args, **kwargs) -> Iterable[Dict]:
        feature = self.feature_columns['feature']
        self._batch.append(element)
        if len(self._batch) >= self.batch_size:
            for data_processed in self.nltk_analyzer(elements=self._batch, column=feature):
                yield data_processed
            self._batch = []

    def setup(self):

        if self.sentiment_analyzer is None:
            self.sentiment_analyzer = SentimentNltk()

    def start_bundle(self):

        self._batch = []

    def finish_bundle(self):

        feature = self.feature_columns['feature']
        if len(self._batch) != 0:
            for data_processed in self.nltk_analyzer(elements=self._batch, column=feature):
                yield data_processed
        self._batch = []



class SentimentAnalysis(beam.PTransform):

    def __init__(self, feature_columns: dict = None, batch_size: int = 1000):
        self.feature_columns = feature_columns
        self.batch_size = batch_size

    def expand(self, pcoll: beam.PCollection) -> beam.PTransform:
        sentiment_analyser = pcoll | "SentimentAnalyser" >> beam.ParDo(SentimentAnalyser(
            feature_columns=self.feature_columns,
            batch_size=self.batch_size
        ))
        return sentiment_analyser
