import numpy as np


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
    """Document-to-vector matching."""
    def doc2vec_score(self, model, k=30):
        return 0.0

