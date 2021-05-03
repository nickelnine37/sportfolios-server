from firebase_admin import credentials, initialize_app
import os

ADMIN_SDK_KEY = 'sportfolios-431c6-firebase-adminsdk-bq76v-f490ad544c.json'
BASE_DIR='/var/www'

cred = credentials.Certificate(os.path.join(BASE_DIR, ADMIN_SDK_KEY))
default_app = initialize_app(cred)