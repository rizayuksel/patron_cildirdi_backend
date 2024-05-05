from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
import urllib

app = Flask(__name__)

DB_HOST = 'hr-task-db.cqbarc8xc1jj.us-east-1.rds.amazonaws.com'
DB_PORT = 3306
DB_NAME = 'hr-task'
DB_USER = 'hr-task-user'
DB_PASSWORD = urllib.parse.quote('adinTask2024!')

# engine = create_engine(f"mysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=True)
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=True)

Base = declarative_base()
Session = sessionmaker(bind=engine)

class DailyCampaign(Base):
    __tablename__ = 'tbl_daily_campaigns'

    id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer)
    campaign_name = Column(String)
    date = Column(Date)
    views = Column(Integer)
    impressions = Column(Integer)
    cpm = Column(Integer)
    clicks = Column(Integer)

class DailyScore(Base):
    __tablename__ = 'tbl_daily_scores'

    id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer)
    date = Column(Date)
    media_score = Column(Integer)
    creative_score = Column(Integer)
    effectiveness_score = Column(Integer)

@app.route('/campaign_data', methods=['GET'])
def get_campaign_data():
    # Query parameters
    campaign_id = request.args.get('campaign_id')
    start_date = datetime.strptime(request.args.get('start_date'), '%Y-%m-%d').date()
    end_date = datetime.strptime(request.args.get('end_date'), '%Y-%m-%d').date()

    session = Session()

    import ipdb; ipdb.set_trace()

    if campaign_id is None:
        query = session.query(DailyCampaign, DailyScore).join(DailyScore, DailyCampaign.campaign_id == DailyScore.campaign_id).filter(
            DailyCampaign.date.between(start_date, end_date)
        ).all()
    else:
        query = session.query(DailyCampaign, DailyScore).join(DailyScore, DailyCampaign.campaign_id == DailyScore.campaign_id).filter(
            DailyCampaign.campaign_id == campaign_id,
            DailyCampaign.date.between(start_date, end_date)
        ).all()

    session.close()

    response = []
    for campaign, score in query:
        response.append({
            'campaign_id': campaign.campaign_id,
            'campaign_name': campaign.campaign_name,
            'date': campaign.date.strftime('%Y-%m-%d'),
            'views': campaign.views,
            'impressions': campaign.impressions,
            'cpm': campaign.cpm,
            'clicks': campaign.clicks,
            'media_score': score.media_score,
            'creative_score': score.creative_score,
            'effectiveness_score': score.effectiveness_score
        })

    return jsonify(response)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)



