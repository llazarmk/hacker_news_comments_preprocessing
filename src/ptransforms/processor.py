import apache_beam as beam
from dofn.text_processing import TextAnalyser


class HNCommentsProcessing(beam.PTransform):

    def __init__(self,
                 feature_columns: dict = None,
                 batch_size: int = 100):
        """

        :param query: string query
        :param feature_columns:
               feature_columns = {'id': 'comment_id','feature': 'comment_text','output_prefix': 'comment'}
        """
        super(HNCommentsProcessing, self).__init__()
        self._feature_columns = feature_columns
        self._batch_size = batch_size

    def expand(self, pcollection) -> beam.PTransform:
        text_analyser = (pcollection
                         | "TextProcessing" >> beam.ParDo(TextAnalyser(feature_columns=self._feature_columns,
                                                                       batch_size=self._batch_size
                                                                       )))
        return text_analyser


