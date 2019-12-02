

# MAG
- [MAG Coverage](https://arxiv.org/pdf/1703.05539.pdf)

# Detecting author duplicates

## KDD Cup 2013 

### Track 1
- [kaggle](https://www.kaggle.com/c/kdd-cup-2013-author-paper-identification-challenge/discussion)
- [1st place](https://www.csie.ntu.edu.tw/~cjlin/papers/kddcup2013/)
- [1st place code](https://github.com/kdd-cup-2013-ntu/track1)
- [2nd place](https://www.researchgate.net/publication/262320550_KDD_Cup_2013_-_Author-paper_identification_challenge_Second_place_team)
- [2nd place code](https://github.com/lucaseustaquio/kdd-cup-2013-track1)

### Track 2
- [kaggle](https://www.kaggle.com/c/kdd-cup-2013-author-disambiguation/discussion)
- [1st place](https://www.csie.ntu.edu.tw/~cjlin/papers/kddcup2013/)
- [1st place code](https://github.com/kdd-cup-2013-ntu/track2)
- [2nd place code](https://github.com/remenberl/KDDCup2013)
- [4th place code](https://github.com/bensolucky/Authors)
- [general methodology](https://www.kaggle.com/c/kdd-cup-2013-author-disambiguation/discussion/4843#latest-26350)

## How Microsoft tackled the problem
- [Author claims](https://www.microsoft.com/en-us/research/project/academic/articles/microsoft-academic-uses-knowledge-address-problem-conflation-disambiguation/)
- [ALIAS](https://pdfs.semanticscholar.org/2657/8e374a7fcbf36d076429d6bc51e5f982700f.pdf?_ga=2.34106022.62442393.1555352054-819732554.1555352054)
- [Author name disambiguation](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/02/2013-wsdm-p495-liu.pdf)

# Estimate on the number of duplicates
https://www.kaggle.com/c/kdd-cup-2013-author-disambiguation/discussion/4777#latest-25295
M = N(1 - F1) / (1 - 0.66)
N - number of authors in database
F1 - baseline F1 score
total_N = 247203	
private_N = 197762 
private_F1_baseline = 0.95375
public_N = 49440
public_F1_baseline = 0.94411
