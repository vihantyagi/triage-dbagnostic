"""rename production schema and list_predcitons to triage_predcition and predictions 

Revision ID: cdd0dc9d9870
Revises: 670289044eb2
Create Date: 2021-04-13 00:53:56.098572

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'cdd0dc9d9870'
down_revision = '670289044eb2'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(text("CREATE SCHEMA IF NOT EXISTS triage_production"))
    op.execute(text("ALTER TABLE production.list_predictions SET SCHEMA triage_production;"))
    op.execute(text("ALTER TABLE production.prediction_metadata SET SCHEMA triage_production"))
    op.execute(text("ALTER TABLE triage_production.list_predictions RENAME TO predictions"))
 

def downgrade():
    op.execute(text("ALTER TABLE triage_production.predictions SET SCHEMA production;"))
    op.execute(text("ALTER TABLE triage_production.prediction_metadata SET SCHEMA production"))
    op.execute(text("ALTER TABLE production.predictions RENAME TO list_predictions"))
    op.execute(text("DROP SCHEMA IF EXISTS triage_production"))
