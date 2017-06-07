import numpy as np
import math


class AveragePrecision(object):

    def __init__(self, relevant_ls, prediction_ls):
        self.rlvt_ls = relevant_ls
        self.pred_ls = prediction_ls

    """Brute force matching."""
    def bf_score(self, k=30):

        match = [1 if p in self.rlvt_ls else 0 for p in self.pred_ls[:k]]

        score = []
        pos = 0
        for i, m in enumerate(match):
            pos += m
            score.append(float(pos) / float(i + 1))

        return np.mean(score)


# TODO
"""Document-to-vector NDCG."""
def doc2vec_ndcg(topic_words, predict_words_ls, model, k=30, ideal=0.8):
    rel = [model.docvecs.similarity_unseen_docs(model, topic_words, predict_words)
                for predict_words in predict_words_ls[:k]
    ]

    dcg = rel[0]
    for i, r in enumerate(rel[1:], 2):
        dcg += (r / math.log2(i))

    icdg = ideal
    for i in range(2, 2 + len(rel[1:])):
        icdg += (ideal / math.log2(i))

    print('@@@NDCG', dcg, icdg)

    return dcg / icdg

