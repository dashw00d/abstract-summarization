# Sumy LexRank Imports
from sumy.summarizers.lex_rank import LexRankSummarizer 
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer


def lex_sum(text, limit):
	parser = PlaintextParser.from_string(text, Tokenizer("english"))

	summarizer = LexRankSummarizer()

	#Summarize the document with 2 sentences
	summary = summarizer(parser.document, limit) 

	return [str(sentence) for sentence in summary]


if __name__=="__main__":

	print(lex_sum('this is a test. this is another test. How many tests do I need? I dont know, just keep testing', 2))