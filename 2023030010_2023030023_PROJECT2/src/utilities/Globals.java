package utilities;


public class Globals {
    
    public static final int maxNumOfStudents = 10;
    public static final int maxNumOfCourses = 5;
    
    public static final String separator = "=============================================================";
    public static final String mainPrompt = "Choose what do you want to do:\n"
                                                + "Type \"S\" to manage students\n"
                                                + "Type \"C\" to manage courses\n"
                                                + "Type \"A\" for course assingment to students\n"
                                                + "Type \"Q\" to quit\n";
    
    public static final String studentPrompt = "Type \"I\" to insert student\n"
                                                + "Type \"U\" to update student\n"
                                                + "Type \"D\" to delete student\n"
                                                + "Type \"R\" for student report\n"
                                                + "Type \"Q\" to quit\n";
    
    public static final String coursePrompt = "Type \"I\" to insert course\n"
                                                + "Type \"U\" to update course\n"
                                                + "Type \"D\" to delete course\n"
                                                + "Type \"R\" for course report\n"
                                                + "Type \"Q\" to quit\n";
    
    public static final String assignmentPrompt = "Type \"I\" to assign course to student\n"
                                                + "Type \"D\" to unassign course to student\n"
                                                + "Type \"Q\" to quit\n";
    
    public static final String insertStudentPrompt = "Give student AM, student Name, student age "
                                                     + "separated by white spaces";
    public static final String updateStudentPrompt = "Give student AM, student new Name, student new age "
                                                     + "separated by white spaces or type null/-1 to leave one of the fields as is";
    public static final String deleteStudentPrompt = "Give the AM of the student you want to delete";
    public static final String reportStudentPrompt = "Give the AM of the student you want to report on";
    
    public static final String insertCoursePrompt = "Give course Code, course Title and semester "
                                                     + "separated by white spaces";
    public static final String updateCoursePrompt = "Give course Code, course new Title, course new semester "
                                                     + "separated by white spaces or type null/-1 to leave one of the fields as is";
    public static final String deleteCoursePrompt = "Give the code of the course you want to delete";
    public static final String reportCoursePrompt = "Give the code of the course you want to report on";
    
    public static final String assignCoursePrompt = "Give student AM and course Code "
                                                     + "separated by white spaces";
    public static final String unassignCoursePrompt = "Give the AM of the student and the code of the course you want to unassign";
    
    public static final int ageLowerLimit = 17;
    public static final int ageUpperLimit = 60;
    public static final int minSemester = 1;
    public static final int maxSemester = 10;
    
    
    /* view messages */
    public static final String didNotFindAnything = "Your search returned no results";
    public static final String invalidParams = "You probably tried to enter invalid parameters";
    public static final String sthWrong = "Something went wrong";
    public static final String rexExists = "A similar record seems to exist";
    
}
