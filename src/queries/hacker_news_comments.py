def get_hacker_news_comments_query(table):
    query = f"""SELECT comments.id AS comment_id,
                      comments.parent AS comment_story_id,
                      comments.text AS comment_text,
                      comments.by AS comment_author,
                      DATE(comments.timestamp) AS comment_date
                FROM `{table}` as comments 
                WHERE comments.text IS NOT NULL AND RAND() <= 0.1
                
            """
    return query
