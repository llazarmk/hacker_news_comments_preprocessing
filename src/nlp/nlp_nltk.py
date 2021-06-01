import nltk

try:
    from nltk.corpus import stopwords as nltk_stopwords
except ImportError:
    # nltk.download(['punkt', 'stopwords', 'vader_lexicon',])  # 'brown' corpora might be needed
    from nltk.corpus import stopwords as nltk_stopwords

# Initialize the VADER sentiment analyzer
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class SentimentNltk(object):

    def __init__(self):
        self.value = ""
        self.analyzer = SentimentIntensityAnalyzer()

    def get_polarity_score(self, text: str) -> dict:
        """

        :param text: string
        :return: {'neg':0.0,'neu':0.67,'pos':0.324,'compound':0.987}
        """
        return self.analyzer.polarity_scores(text=text)

    def process(self, text: str):
        analyzer = self.get_polarity_score(text=text)
        return analyzer




