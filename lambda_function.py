import json
import hashlib
import logging
from sqlalchemy import create_engine, text

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database configuration
DATABASE_URL = "postgresql://postgres:H{Qxy_oLvT{hN#yJ3p(7nD<6M2wu@jobinhood-db.c96iwgeuwlji.us-east-1.rds.amazonaws.com:5432/postgres"
db_engine = create_engine(DATABASE_URL)

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract user email from the event
        user_email = event['request']['userAttributes']['email'].lower()
        
        # Hash the email to create a unique user_id
        user_id = hashlib.sha256(user_email.encode()).hexdigest()
        
        # Prepare the SQL query to insert or update the user
        query = """
            INSERT INTO "Users" (user_id, email, tokens)
            VALUES (:user_id, :email, :tokens)
            ON CONFLICT (user_id) DO UPDATE
            SET email = EXCLUDED.email
        """
        
        # Execute the query
        with db_engine.connect() as connection:
            connection.execute(text(query), {
                'user_id': user_id,
                'email': user_email,
                'tokens': 3  # Initial tokens
            })

        logger.info(f"Successfully added user to database: {user_email}")

    except Exception as e:
        logger.error(f"Error processing event: {str(e)}")
        # We're not modifying the event, just logging the error

    # For Post Confirmation trigger, we just return the event as is
    logger.info(f"Returning event: {json.dumps(event)}")
    return event