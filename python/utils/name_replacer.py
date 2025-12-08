import spacy


class NameReplacer:
    def __init__(self, replace_name: str, new_name: str):
        self.replace_name = replace_name
        self.new_name = new_name
        # Загружаем модели для обоих языков
        try:
            self.nlp_ru = spacy.load("ru_core_news_sm")
        except OSError:
            raise Exception("Установите русскую модель: python -m spacy download ru_core_news_sm")

        try:
            self.nlp_en = spacy.load("en_core_web_sm")
        except OSError:
            raise Exception("Установите английскую модель: python -m spacy download en_core_web_sm")

    def detect_language(self, text):
        ru_chars = len([c for c in text if 'а' <= c <= 'я' or 'А' <= c <= 'Я'])
        en_chars = len([c for c in text if 'a' <= c <= 'z' or 'A' <= c <= 'Z'])
        return 'ru' if ru_chars > en_chars else 'en'

    def replace_names(self, text):
        lang = self.detect_language(text)
        nlp = self.nlp_ru if lang == 'ru' else self.nlp_en

        doc = nlp(text)
        result = []

        for token in doc:
            if (token.text.lower() == self.replace_name.lower()):
                if token.text.istitle():
                    result.append(self.new_name.title())
                else:
                    result.append(self.new_name)
            else:
                result.append(token.text)
            result.append(token.whitespace_)

        return ''.join(result)

if __name__ == "__main__":
    name_replacer = NameReplacer("harry", "man")
    print(name_replacer.replace_names("Anyway -- Harry"))