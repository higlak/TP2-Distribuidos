class Review():
    def __init__(self, id, title, price, user_id, profileName, helpfulness, score, time, summary, text):
        self.id = id
        self.title = title
        self.price = price
        self.user_id = user_id
        self.profileName = profileName
        self.helpfulness = helpfulness
        self.score = score
        self.time = time
        self.summary = summary
        self.text = text

    def to_csv(self):
        return f'{self.id},{self.title},{self.price},{self.user_id},{self.profileName},{self.helpfulness},{self.score},{self.time},{self.summary},{self.text}'