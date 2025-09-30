from flask import jsonify

def ok(data=None, message="ok", status = 200):
    return jsonify(
        success     = True,
        message     = message,
        data        = data,
        error       = None
    ),
    status
    
def fail(message="error", status=400, error=None):
    return jsonify(
        success     = False,
        message     = message,
        data        = None,
        error       = error    
    ),
    status