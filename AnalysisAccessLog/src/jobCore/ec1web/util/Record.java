package jobCore.ec1web.util;

import static jobUtil.GlobalTool.TAB;
/**
 * 資料紀錄的物件
 * Created by Joan on 2014/9/16.
 */
public class Record {
    String site;
    String page;
    String action;
    int PCcount = 0, PhoneCount = 0, TabletCount = 0, OtherCount = 0, UnknownCount = 0;

    public Record(){}

    public Record(String site, String page, String action) {
        this.site = site;
        this.page = page;
        this.action = action;
    }

    public String getSite() {
        return site;
    }

    public String getPage() {
        return page;
    }

    public String getAction() {
        return action;
    }

    public int getPCcount() {
        return PCcount;
    }

    public int getPhoneCount() {
        return PhoneCount;
    }

    public int getTabletCount() {
        return TabletCount;
    }

    public int getOtherCount() {
        return OtherCount;
    }

    public int getUnknownCount() {
        return UnknownCount;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public void plusPCcount(int PCcount) {
        this.PCcount += PCcount;
    }

    public void plusPhoneCount(int phoneCount) {
        this.PhoneCount += phoneCount;
    }

    public void plusTabletCount(int tabletCount) {
        this.TabletCount += tabletCount;
    }

    public void plusOtherCount(int otherCount) {
        this.OtherCount += otherCount;
    }

    public void plusUnknownCount(int unknownCount) {
        this.UnknownCount += unknownCount;
    }

    public int getTotal() {
        return PCcount + PhoneCount + TabletCount + OtherCount + UnknownCount;
    }

    @Override
    public String toString() {
        int pc = 0, mobile = 0;
        if (site.equals("mobile"))
            mobile = getTotal();
        else
            pc = getTotal();

        return site + TAB + page + TAB + action + TAB + pc + TAB + mobile + TAB + PCcount + TAB + PhoneCount + TAB + TabletCount + TAB + OtherCount + TAB + UnknownCount;
    }
}
