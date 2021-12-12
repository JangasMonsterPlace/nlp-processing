import pandas as pd
import gensim

from typing import List

from gensim.corpora import Dictionary

from orm import ORM, LDA


def preprocess_lda(tokens: List[List[str]]):
    id2word = Dictionary(tokens)
    corpus = [id2word.doc2bow(doc) for doc in tokens]

    return id2word, corpus


def create_lda_model(id2word):
    lda_model = gensim.models.ldamodel.LdaModel(
        id2word=id2word,
        num_topics=4,
        random_state=42,
        passes=10,
        update_every=1,
        alpha='auto'
    )

    return lda_model


def extract_dominant_topics(ldamodel, corpus, texts, job_id, threshhold: float = 0.6) -> List[LDA]:
    # Init output
    sent_topics_df = pd.DataFrame()

    # Get main topic in each document
    for i, row in enumerate(ldamodel[corpus]):
        row = sorted(row, key=lambda x: (x[1]), reverse=True)
        # Get the Dominant topic, Perc Contribution and Keywords for each document
        for j, (topic_num, prop_topic) in enumerate(row):
            if j == 0:  # => dominant topic
                wp = ldamodel.show_topic(topic_num)
                topic_keywords = ", ".join([word for word, prop in wp])
                sent_topics_df = sent_topics_df.append(pd.Series([int(topic_num), round(prop_topic,4), topic_keywords]), ignore_index=True)
            else:
                break
    sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']

    # Add original text to the end of the output
    contents = pd.Series(texts)
    sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)

    df_dominant_topic = sent_topics_df.reset_index()
    df_dominant_topic.columns = ['no', 'topic_id', 'perc', 'keywords', 'text']

    filtered_df_dict = df_dominant_topic[df_dominant_topic["perc"] > threshhold][['topic_id', 'text']].to_dict('records')
    return [LDA(job_id=job_id, **e) for e in filtered_df_dict]


def lda_runner(entities: List[str], tokens: List[List[str]], job_id: int):
    id2word, corpus = preprocess_lda(tokens)
    model = create_lda_model(id2word)
    ldas = extract_dominant_topics(model, corpus, entities, job_id)
    ORM.insert_lda(ldas)
