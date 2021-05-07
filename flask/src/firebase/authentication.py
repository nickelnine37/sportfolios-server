import os
from firebase_admin import auth
import hashlib


def verify_user_token(token: str):
    """
    Attempt to verify a firebase user id token. If successful, return
    True and the user info. If fails, return False along with an error
    message and a response code.

    Params:
        token:      str - Firebase user id token

    Returns:
        success:        bool - whether veriifcation succeeds
            EITHER
        (Error, Code)   (str, int) - error message plus response code
            OR
        info            dict - user info relating to query
    """

    try:
        response = auth.verify_id_token(token)

        # if 'name' not in response:
        #     response['name'] = 'ed'
        #     response['email'] = 'ed@ed.com'
        #     response['email_verified'] = True

        if not response['email_verified']:
            return False ('Not email verified', 401)

        return True, response

    except (auth.InvalidIdTokenError,
            auth.ExpiredIdTokenError,
            auth.RevokedIdTokenError,
            auth.CertificateFetchError) as E:

        print(E)

        if isinstance(E, auth.ExpiredIdTokenError):
            return False, ('Token Expired', 401)

        elif isinstance(E, auth.RevokedIdTokenError):
            return False, ('Token Revoked', 401)

        elif isinstance(E, auth.InvalidIdTokenError):
            return False, ('Malformed Token', 400)

        else:
            return False, ('Certificate Fetch Error', 400)


def verify_admin(passhash: str):

    if not isinstance(passhash, str):
        return False, ('Failed', 401)

    passhashtrue = b'^\x1aG\xbd:\x11\xefu^r\x15m&\r\x1bO\xec\xb1\xf2(\xf7\xc6\x83\x01\x11G$\xc6\x15{.\x18'
    if hashlib.sha256(passhash.encode()).digest() == passhashtrue:
        return True, None

    else:
        return False, ('Failed', 401)


