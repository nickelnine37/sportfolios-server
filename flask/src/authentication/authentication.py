from firebase_admin import auth

def verify_token(token: str):
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

        if 'name' not in response:
            response['name'] = 'ed'
            response['email'] = 'ed@ed.com'

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